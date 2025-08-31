import asyncio, re
from urllib.parse import urljoin
from playwright.async_api import async_playwright, TimeoutError as PwTimeout

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

async def crawl_batch_async(batch: list, concurrency: int = 5, headless: bool = True) -> list:
    # 하나의 브라우저/컨텍스트 재사용 + 페이지를 병렬로 여러 개
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
                return await _crawl_one(ctx, dict(item))  # 원본 보호

        results = await asyncio.gather(*[asyncio.create_task(runner(it)) for it in batch])
        await ctx.close()
        await browser.close()
        return results

# 로컬 테스트
if __name__ == "__main__":
    batch = [
        {"requestUrl": "/domestic/stock/032280/total", "requestEmail": "test@test.com"},
        {"requestUrl": "/worldstock/etf/AAPX.K", "requestEmail": "test@test.com"},
        # 필요하면 더 추가
    ]
    out = asyncio.run(crawl_batch_async(batch, concurrency=5))
    from pprint import pprint; pprint(out)
