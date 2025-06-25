from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import pandas as pd
import os

API_KEY = "SnBBHQMK0OnueWeZt8NzhYvMs9M4dCJpcxERGhSQQ1Qj8pmX3OzBln%2B87ZLva3LIxiRdXPxOWC%2BT7i3INfWn8g%3D%3D"
URL = "http://apis.data.go.kr/1360000/VilageFcstInfoService_2.0/getUltraSrtNcst"

def fetch_weather(**kwargs):
    base_date = datetime.now().strftime("%Y%m%d")
    base_time = datetime.now().strftime("%H00")

    params = {
        "serviceKey": API_KEY,
        "pageNo": "1",
        "numOfRows": "1000",
        "dataType": "XML",
        "base_date": base_date,
        "base_time": base_time,
        "nx": "60",  # 서울시청
        "ny": "127"
    }
    res = requests.get(URL, params=params, timeout=10)
    data = xmltodict.parse(res.text)
    items = data["response"]["body"]["items"]["item"]
    kwargs["ti"].xcom_push(key="raw_weather", value=items)

def parse_weather(**kwargs):
    items = kwargs["ti"].xcom_pull(key="raw_weather", task_ids="extract_weather")
    df = pd.DataFrame(items)
    kwargs["ti"].xcom_push(key="df_weather", value=df.to_dict())

def save_weather(**kwargs):
    df_dict = kwargs["ti"].xcom_pull(key="df_weather", task_ids="transform_weather")
    df = pd.DataFrame.from_dict(df_dict)
    os.makedirs("/opt/airflow/data", exist_ok=True)
    filename = f"/opt/airflow/data/weather_{datetime.now().strftime('%Y%m%d_%H%M')}.csv"
    df.to_csv(filename, index=False, encoding="utf-8-sig")

def build_dag():
    default_args = {
        "owner": "airflow",
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    }
    with DAG(
        dag_id="weather_etl_split",
        default_args=default_args,
        schedule_interval="@hourly",
        start_date=datetime(2025, 1, 1),
        catchup=False,
        tags=["weather"],
    ) as dag:
        t1 = PythonOperator(task_id="extract_weather", python_callable=fetch_weather)
        t2 = PythonOperator(task_id="transform_weather", python_callable=parse_weather)
        t3 = PythonOperator(task_id="load_weather", python_callable=save_weather)

        t1 >> t2 >> t3

        return dag

globals()["weather_etl_split"] = build_dag()