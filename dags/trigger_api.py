# dags/trigger_spring_api.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import os
from dotenv import load_dotenv

load_dotenv()

API_SERVER_HOST = os.getenv("API_SERVER_HOST")

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(seconds=15),
}

def call_spring_api():
    url = f"{API_SERVER_HOST}/api/v1/external/alarms/publish"
    try:
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            print("✅ API 호출 성공")
        else:
            print(f"❌ 호출 실패: {response.status_code}")
    except Exception as e:
        print(f"❌ API 호출 중 오류: {e}")

with DAG(
    dag_id='publish_dag',
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval='* * * * *',  # 1분마다
    catchup=False,
    tags=['spring', 'api'],
    max_active_runs=1,
) as dag:
    
    trigger = PythonOperator(
        task_id='trigger_api_task',
        python_callable=call_spring_api
    )



if __name__ == "__main__":
    call_spring_api()