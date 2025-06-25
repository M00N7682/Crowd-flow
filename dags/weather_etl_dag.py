from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from public_data.fetch import fetch_weather
from public_data.parse import parse_weather
from public_data.save import save_weather

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="weather_etl_pipeline",
    default_args=default_args,
    description="기상청 초단기실황 수집 DAG (fetch → parse → save)",
    schedule_interval="@hourly",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["weather", "public_data"],
) as dag:
    
    t1 = PythonOperator(
        task_id="fetch_weather",
        python_callable=fetch_weather,
    )

    t2 = PythonOperator(
        task_id="parse_weather",
        python_callable=parse_weather,
    )

    t3 = PythonOperator(
        task_id="save_weather",
        python_callable=save_weather,
    )

    t1 >> t2 >> t3
