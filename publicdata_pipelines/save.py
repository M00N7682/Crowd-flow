# save.py
import os
from datetime import datetime

def save_csv(df, nx, ny, base_date, base_time, output_dir="/opt/airflow/data"):
    os.makedirs(output_dir, exist_ok=True)
    filename = f"weather_{nx}_{ny}_{base_date}{base_time}.csv"
    path = os.path.join(output_dir, filename)
    df.to_csv(path, index=False, encoding="utf-8-sig")
    print(f"[✓] 저장 완료: {path}")
