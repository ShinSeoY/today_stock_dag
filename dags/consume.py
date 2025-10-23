from airflow.decorators import dag, task
from datetime import datetime, timedelta, timezone
from kafka import KafkaConsumer, KafkaProducer
from kafka import TopicPartition
from playwright.sync_api import sync_playwright
import json, os, time, asyncio, aiohttp
from playwright.async_api import async_playwright, TimeoutError as PwTimeout
import nest_asyncio
import re
from urllib.parse import urljoin

# 환경 변수
KAFKA_HOST = os.getenv("KAFKA_HOST")
# KAFKA_BROKERS = [f'{KAFKA_HOST}1:19091', f'{KAFKA_HOST}2:19092', f'{KAFKA_HOST}3:19093']
KAFKA_BROKERS = [KAFKA_HOST]
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")
KAFKA_SUCCESS_TOPIC = os.getenv("KAFKA_SUCCESS_TOPIC")
KAFKA_FAIL_TOPIC = os.getenv("KAFKA_FAIL_TOPIC")
API_SERVER_HOST = os.getenv("API_SERVER_HOST")
MAX_BUFFER_SIZE = 10
TIMEOUT = 55  # 초

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(seconds=10),
}

def add_error(item: dict, stage: str, exc: Exception | str):
    if not isinstance(exc, str):
        exc = str(exc)
    item.setdefault("errors", []).append({
        "stage": stage,
        "message": exc,
        "ts": datetime.now(timezone.utc).isoformat()
    })

def has_required_keys(d: dict, keys=("requestUrl", "requestEmail")):
    missing = [k for k in keys if k not in d]
    return missing, len(missing) == 0

PRICE_SELECTORS = [
    'strong[class^="GraphMain_price"]',
    'strong[class*=" GraphMain_price"]',
    'strong[class^="Price_article__"]',
    'strong[class*=" Price_article__"]',
]

def get_valid_price(txt) -> str | None:
    if not txt:
        return None
    m = re.search(r'\d[\d,\.]*', txt)
    if not m:
        return None
    return re.sub(r'[,]', '', m.group(0)) 

async def _extract_price(page) -> str | None:
    for sel in PRICE_SELECTORS:
        try:
            loc = page.locator(sel).first 
            await loc.wait_for(state="visible", timeout=8000)
            txt = (await loc.inner_text() or "").strip()
            v = get_valid_price(txt)
            if v:
                return v
        except Exception:
            continue

async def _crawl_one(ctx, item: dict) -> dict:
    page = await ctx.new_page()
    try:
        url = urljoin("https://m.stock.naver.com/", item["requestUrl"].lstrip("/"))
        await page.goto(url, timeout=45_000, wait_until="domcontentloaded")
        
        price = await _extract_price(page)
        if not price:
            # 1회 재시도
            await page.reload(wait_until="load", timeout=20_000)
            price = await _extract_price(page)

        if price:
            item["collectedPrice"] = price
            item["crawled"] = True
        else:
            item.setdefault("errors", []).append({"stage":"crawl","message":"price not found"})
            item["collectedPrice"] = None
            item["crawled"] = False
        return item
    except Exception as e:
        item.setdefault("errors", []).append({"stage":"crawl","message":str(e)})
        item["collectedPrice"] = None
        item["crawled"] = False
        return item
    finally:
        await page.close()

"""크롤링 완료 후 조건 확인하여 이메일 발송이 필요한 데이터만 필터링"""
def valid_batch(batch: list) -> list:
    result = []
    
    for item in batch:
        if not isinstance(item, dict):
            continue

        if not item.get('crawled') or not item.get('collectedPrice'):
            continue
            
        try:
            req_price = item.get('requestPrice')
            col_price = item.get('collectedPrice')
            condition = item.get('conditionType')
            
            if not all([req_price, col_price, condition]):
                continue
                
            req = float(req_price)
            col = float(col_price)
            
            if condition == 'GTE' and col >= req:
                result.append(item)
            elif condition == 'LTE' and col <= req:
                result.append(item)
                
        except (ValueError, TypeError) as e:
            add_error(item, "valid_batch", f"Invalid number format: {e}")
            continue
            
    return result

async def crawl_batch_async(batch: list, concurrency: int = 5, headless: bool = True) -> list:
    async with async_playwright() as p:
        device = p.devices.get("iPhone 13 Pro") or p.devices["iPhone 12 Pro"]
        browser = await p.chromium.launch(
            headless=headless,
            args=["--no-sandbox", "--disable-gpu"]
        )
        ctx = await browser.new_context(**device, locale="ko-KR", timezone_id="Asia/Seoul")

        sem = asyncio.Semaphore(concurrency)
        async def runner(item):
            async with sem:
                return await _crawl_one(ctx, dict(item))
            
        results = await asyncio.gather(*[asyncio.create_task(runner(it)) for it in batch])
        await ctx.close()
        await browser.close()
        return results

@dag(
    dag_id='consume_and_process_dag',
    start_date=datetime(2025, 8, 3),
    schedule_interval='* * * * *',
    catchup=False,
    default_args=default_args,
    tags=['kafka', 'batch', 'email'],
    max_active_runs=2,
)
def kafka_batch_dag():
    
    # Kafka 메시지 배치 단위로 수신
    @task
    def poll_msg():
        try:
            print("=== Kafka Consumer 시작 (수동 할당 방식) ===")
            consumer = KafkaConsumer(
                bootstrap_servers=KAFKA_BROKERS,
                # group_id 제거 - 수동 할당에는 불필요
                auto_offset_reset='earliest',
                enable_auto_commit=False,
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                fetch_min_bytes=1,
                fetch_max_wait_ms=500,
                consumer_timeout_ms=10000
            )
            
            # 파티션 수동 할당 - Coordinator 불필요!
            tp = TopicPartition(KAFKA_TOPIC, 0)
            consumer.assign([tp])
            consumer.seek_to_beginning(tp)
            
            print(f"✓ 파티션 수동 할당: {consumer.assignment()}")
            
            # 오프셋 정보 확인
            start_offset = consumer.beginning_offsets([tp])[tp]
            end_offset = consumer.end_offsets([tp])[tp]
            current_position = consumer.position(tp)
            
            print(f"\n📊 파티션 0 정보:")
            print(f"  시작 오프셋: {start_offset}")
            print(f"  현재 위치: {current_position}")
            print(f"  끝 오프셋: {end_offset}")
            print(f"  읽을 수 있는 메시지: {end_offset - current_position}개")
            
            if end_offset - current_position == 0:
                print("\n⚠️  토픽에 메시지가 없습니다!")
                consumer.close()
                return []
            
            end_time = time.monotonic() + TIMEOUT
            buffer, batches = [], []
            message_count = 0

            print("\n=== 메시지 폴링 시작 ===")
            while time.monotonic() < end_time:
                polled = consumer.poll(timeout_ms=1000)
                
                if not polled:
                    # 메시지를 이미 읽었으면 종료
                    if message_count > 0:
                        print("\n✓ 모든 메시지 읽기 완료!")
                        break
                    # 10초 대기 후 종료
                    if time.monotonic() - (end_time - TIMEOUT) > 10:
                        print("\n⚠️  10초 동안 메시지 없음 - 종료")
                        break
                    print(".", end="", flush=True)
                    continue
                
                print(f"\n✅ 폴링 성공!")
                
                for tp_key, msgs in polled.items():
                    print(f"\n파티션 {tp_key.partition}에서 {len(msgs)}개 메시지 수신:")
                    for m in msgs:
                        message_count += 1
                        data = m.value
                        
                        print(f"  메시지 #{message_count}:")
                        print(f"    오프셋: {m.offset}")
                        print(f"    키: {list(data.keys()) if isinstance(data, dict) else 'N/A'}")
                        
                        missing, ok = has_required_keys(data)
                        if not ok:
                            print(f"    ⚠️  누락된 키: {missing}")
                            add_error(data, "poll_msg", f"missing keys: {missing}")
                        else:
                            print(f"    ✓ 필수 키 확인 완료")
                        
                        buffer.append(data)
                        
                        if len(buffer) >= MAX_BUFFER_SIZE:
                            print(f"\n📦 배치 #{len(batches)+1} 생성 (크기: {len(buffer)})")
                            batches.append(buffer.copy())
                            buffer.clear()

            if buffer:
                print(f"\n📦 마지막 배치 생성 (크기: {len(buffer)})")
                batches.append(buffer.copy())

            print(f"\n\n=== 폴링 완료 ===")
            print(f"총 메시지 수: {message_count}")
            print(f"총 배치 수: {len(batches)}")
            
            if batches:
                print(f"배치 내용:")
                for i, batch in enumerate(batches):
                    print(f"  배치 #{i+1}: {len(batch)}개 항목")
                    if batch:
                        print(f"    첫 항목: {batch[0]}")
            
            consumer.close()
            print("✓ Consumer 정상 종료")
            
            return batches

        except Exception as e:
            print(f"\n❌ 에러: {str(e)}")
            import traceback
            traceback.print_exc()
            err_item = {"errors": [], "stage": "poll_msg"}
            add_error(err_item, "poll_msg", e)
            return [[err_item]]

    @task
    def process_batch(batch: list):
        try:
            return asyncio.run(crawl_batch_async(batch))
        except Exception as e:
            # 이 단계에서 실패해도 다음 단계로 넘겨 DLQ로 떨어지게 함
            safe = []
            for it in (batch or []):
                it = dict(it) if isinstance(it, dict) else {"raw": it}
                add_error(it, "process_batch", e)
                it.setdefault("crawled", False)
                it.setdefault("collectedPrice", None)
                safe.append(it)
            # 비어있으면 더미 하나라도 보냄
            if not safe:
                safe = [{"errors": [], "crawled": False, "collectedPrice": None}]
                add_error(safe[0], "process_batch", e)
            return safe


    # 이메일 발송 분리 Task
    @task
    def send_email_batch(batch: list):
        async def send_all():
            try:
                # 조건에 맞는 아이템만 필터링
                valid_items = valid_batch(batch)
                
                producer = KafkaProducer(
                    bootstrap_servers=KAFKA_BROKERS,
                    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8')
                )
                
                # 조건에 맞지 않는 아이템들은 성공 처리 (이메일 발송 불필요)
                for item in batch:
                    if item in valid_items:
                        continue

                    item["emailed"] = False
                    item["skipReason"] = "condition not met"

                    # 재시도 메타 (루프 방지용)
                    rc = int(item.get("retryCount", 0))
                    item["retryCount"] = rc + 1
                    
                    try:
                        producer.send(KAFKA_TOPIC, value=item)
                    except Exception as e:
                        add_error(item, "requeue", e)
                    
                timeout = aiohttp.ClientTimeout(total=30)
                async with aiohttp.ClientSession(timeout=timeout) as session:
                    # 조건에 맞는 아이템들만 실제 이메일 발송
                    for item in valid_items:
                        item.setdefault("emailed", False)
                        try:
                            payload = item.copy()
                            if item.get("collectedPrice"):
                                async with session.post(f"{API_SERVER_HOST}/v1/external/email", json=payload) as resp:
                                    item["emailed"] = (resp.status == 200)
                                    if not item["emailed"]:
                                        add_error(item, "send_email", f"http {resp.status}")
                            else:
                                add_error(item, "send_email", "collectedPrice is empty; skipped email")
                        except Exception as e:
                            add_error(item, "send_email", e)
                            item["emailed"] = False
                            
                return batch
                
            except Exception as e:
                safe = []
                for it in (batch or []):
                    it = dict(it) if isinstance(it, dict) else {"raw": it}
                    add_error(it, "send_email", e)
                    it.setdefault("emailed", False)
                    safe.append(it)
                return safe

        return asyncio.run(send_all())


    # Kafka로 결과 발행
    @task
    def produce_result(batch_results: list):
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKERS,
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8')
        )
        for item in batch_results:
            # 성공 기준: 누적 오류가 없고, 수집/발송이 모두 성공
            errors = item.get("errors", [])
            success = (not errors) and bool(item.get("crawled")) and item.get("collectedPrice")
            item["success"] = success
            topic = KAFKA_SUCCESS_TOPIC if success else KAFKA_FAIL_TOPIC
            try:
                producer.send(topic, value=item)
            except Exception as e:
                # 발행 자체 오류도 누적하고, 실패 토픽으로 재시도
                add_error(item, "produce", e)
                item["success"] = False
                try:
                    producer.send(KAFKA_FAIL_TOPIC, value=item)
                except Exception:
                    pass
        producer.flush()
        producer.close()

    # DAG 실행 순서 (배치실행)
    batches = poll_msg()
    crawled = process_batch.expand(batch=batches)
    emailed = send_email_batch.expand(batch=crawled)
    produce_result.expand(batch_results=emailed)

dag = kafka_batch_dag()
