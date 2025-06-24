from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import pandas as pd
import os

API_KEY = "SnBBHQMK0OnueWeZt8NzhYvMs9M4dCJpcxERGhSQQ1Qj8pmX3OzBln%2B87ZLva3LIxiRdXPxOWC%2BT7i3INfWn8g%3D%3D"
URL = "http://apis.data.go.kr/1360000/VilageFcstInfoService_2.0/getUltraSrtNcst"

def fetch_weather_data():
    params = {
        "serviceKey": API_KEY,
        "dataType": "JSON",
        "base_date": datetime.today().strftime("%Y%m%d"),
        "base_time": "0600",
        "nx": "60",  
        "ny": "127"
    }

    response = requests.get(URL, params=params)
    items = response.json()["response"]["body"]["items"]["item"]

    df = pd.DataFrame(items)
    os.makedirs("/opt/airflow/data", exist_ok=True)
    df.to_csv(f"/opt/airflow/data/weather_{datetime.now().strftime('%Y%m%d%H%M')}.csv", index=False, encoding="utf-8-sig")

def build_dag():
    with DAG(
        dag_id="weather_data_etl",
        start_date=datetime(2024, 1, 1),
        schedule_interval="@hourly"
        catchup=False,
        default_args={
            "owner": "airflow",
            "retries": 1,
            "retry_delay": timedelta(minutes=5),
        },
        tags=["weather", "etl", "fire_prediction"],
    ) as dag:

        task = PythonOperator(
            task_id="fetch_and_save_weather",
            python_callable=fetch_weather_data
        )

        task

    return dag

globals()["weather_data_etl"] = build_dag()
