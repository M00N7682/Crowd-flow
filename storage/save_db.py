"""
save_to_db.py
-------------
정제된 뉴스 데이터를 SQLite DB에 저장하는 모듈
"""

import os
import sqlite3
import pandas as pd
from datetime import datetime


DB_PATH = "data/crowdflow.db"   # DB 파일 경로 (없으면 자동 생성)
TABLE_NAME = "news_articles"


def init_db():
    """SQLite DB 연결 및 테이블 생성"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute(f"""
    CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        title TEXT,
        url TEXT UNIQUE,
        source TEXT,
        published TEXT,
        keyword TEXT,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP
    );
    """)
    conn.commit()
    return conn


def insert_news_from_csv(csv_path: str, keyword: str = ""):
    """정제된 뉴스 CSV를 불러와 SQLite DB에 삽입"""
    if not os.path.isfile(csv_path):
        raise FileNotFoundError(csv_path)

    df = pd.read_csv(csv_path)
    if "url" not in df.columns or "title" not in df.columns:
        raise ValueError("CSV에 'url' 또는 'title' 컬럼이 없습니다.")

    # DB 연결 및 테이블 초기화
    conn = init_db()
    cursor = conn.cursor()

    inserted, skipped = 0, 0
    for _, row in df.iterrows():
        try:
            cursor.execute(f"""
                INSERT INTO {TABLE_NAME} (title, url, source, published, keyword)
                VALUES (?, ?, ?, ?, ?)
            """, (
                row.get("title", ""),
                row.get("url", ""),
                row.get("source", ""),
                row.get("published", ""),
                keyword
            ))
            inserted += 1
        except sqlite3.IntegrityError:
            # URL 중복 → 무시
            skipped += 1

    conn.commit()
    conn.close()
    print(f"[✓] DB 저장 완료: {inserted}건 삽입, {skipped}건 중복 스킵")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="정제된 뉴스 CSV를 SQLite에 저장합니다")
    parser.add_argument("csv_path", help="정제된 CSV 경로 (예: clean_미백_20240622.csv)")
    parser.add_argument("--keyword", help="수집 키워드", default="")
    args = parser.parse_args()

    insert_news_from_csv(args.csv_path, keyword=args.keyword)
