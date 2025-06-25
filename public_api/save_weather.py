import sqlite3
import pandas as pd
from pathlib import Path

DB_PATH = "data/weather.db"
TABLE_NAME = "weather"

def init_db():
    Path("data").mkdir(exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT,
            area TEXT,
            temp REAL,
            humidity REAL,
            wind REAL
        )
    """)
    conn.close()

def insert(data: list[dict]):
    conn = sqlite3.connect(DB_PATH)
    df = pd.DataFrame(data)
    df.to_sql(TABLE_NAME, conn, if_exists="append", index=False)
    conn.close()
    print(f"[✓] Saved {len(df)} rows to {DB_PATH}")

# 사용 예시:
if __name__ == "__main__":
    init_db()
    sample = [
        {"date": "2025-06-24", "area": "서울", "temp": 27.3, "humidity": 65.0, "wind": 2.3},
        {"date": "2025-06-24", "area": "부산", "temp": 25.8, "humidity": 70.2, "wind": 3.1},
    ]
    insert(sample)
