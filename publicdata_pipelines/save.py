# save.py
import os
from datetime import datetime
from pathlib import Path
import pandas as pd
import sqlite3

#CSV 연동 (기본 DAG 구성)
def save_csv(df, nx, ny, base_date, base_time, output_dir="/opt/airflow/data"):
    os.makedirs(output_dir, exist_ok=True)
    filename = f"weather_{nx}_{ny}_{base_date}{base_time}.csv"
    path = os.path.join(output_dir, filename)
    df.to_csv(path, index=False, encoding="utf-8-sig")
    print(f"[✓] 저장 완료: {path}")

#SQLite 연동시 아래 함수로 DAG 수정
def save_to_csv_and_sqlite(df, base_dir=".", table_name="weather"):
    # CSV 저장
    data_dir = Path(base_dir) / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    csv_filename = data_dir / f"weather_{datetime.now().strftime('%Y%m%d_%H%M')}.csv"
    df.to_csv(csv_filename, index=False, encoding="utf-8-sig")

    # SQLite 저장
    db_dir = Path(base_dir) / "db"
    db_dir.mkdir(parents=True, exist_ok=True)
    db_path = db_dir / "weather.db"
    with sqlite3.connect(db_path) as conn:
        df.to_sql(table_name, conn, if_exists="append", index=False)

    print(f"[✓] CSV 저장: {csv_filename}")
    print(f"[✓] SQLite 저장: {db_path}")