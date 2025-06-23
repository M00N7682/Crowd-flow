"""
text_cleansing.py
-----------------
CrowdFlow 전용 뉴스 전처리 모듈
"""

import re
import os
from datetime import datetime, timedelta
from typing import Optional

import pandas as pd
from dateutil.parser import parse as date_parse


# ───────────────────────────────────────────────
# 1) 텍스트 정제 함수
# ───────────────────────────────────────────────
EMOJI_PATTERN = re.compile("[\U00010000-\U0010ffff]", flags=re.UNICODE)
HTML_TAG_PATTERN = re.compile(r"<.*?>")
MULTI_SPACE_PATTERN = re.compile(r"\s+")

def clean_text(text: str) -> str:
    """이모지·HTML 태그·특수문자를 제거하고 공백을 정돈."""
    if not isinstance(text, str):
        return ""

    text = HTML_TAG_PATTERN.sub(" ", text)
    text = EMOJI_PATTERN.sub(" ", text)
    text = re.sub(r"[^\w\s가-힣·]", " ", text)          # 한글·숫자·영문 외 기호 제거
    text = MULTI_SPACE_PATTERN.sub(" ", text).strip()
    return text


# ───────────────────────────────────────────────
# 2) 날짜 표준화 함수
# ───────────────────────────────────────────────
def _from_relative_expression(expr: str) -> Optional[datetime]:
    """
    '1일 전', '3시간 전' 같은 상대 시간을 datetime 객체로 변환.
    지원 단위: 일, 시간, 분
    """
    now = datetime.now()
    if "일 전" in expr:
        days = int(re.search(r"(\d+)", expr).group(1))
        return now - timedelta(days=days)
    if "시간 전" in expr:
        hours = int(re.search(r"(\d+)", expr).group(1))
        return now - timedelta(hours=hours)
    if "분 전" in expr:
        minutes = int(re.search(r"(\d+)", expr).group(1))
        return now - timedelta(minutes=minutes)
    return None


def normalize_date(raw: str) -> str:
    """
    '2024.06.22.', '1일 전', '3시간 전' 등을 'YYYY-MM-DD' 형식으로 변환.
    실패 시 원본 반환.
    """
    raw = raw.strip()

    # 상대 시간 처리
    rel = _from_relative_expression(raw)
    if rel:
        return rel.strftime("%Y-%m-%d")

    # 정규 날짜 처리
    try:
        dt = date_parse(raw, fuzzy=True)
        return dt.strftime("%Y-%m-%d")
    except (ValueError, TypeError):
        return raw  # 변환 실패 시 원본 유지


# ───────────────────────────────────────────────
# 3) 중복 제거 함수
# ───────────────────────────────────────────────
def deduplicate(df: pd.DataFrame) -> pd.DataFrame:
    """title + url 조합으로 중복 제거."""
    before = len(df)
    df = df.drop_duplicates(subset=["title", "url"])
    after = len(df)
    print(f"[Dedup] {before - after} duplicates removed ({after} remaining)")
    return df


# ───────────────────────────────────────────────
# 4) CSV 단위 전체 파이프라인
# ───────────────────────────────────────────────
def preprocess_news_csv(csv_path: str, save: bool = True) -> pd.DataFrame:
    """
    ➀ CSV 로드 → ➁ 텍스트 정제 → ➂ 날짜 표준화 → ➃ 중복 제거
    저장 옵션: `clean_<원본파일명>.csv` 로 저장
    """
    if not os.path.isfile(csv_path):
        raise FileNotFoundError(csv_path)

    df = pd.read_csv(csv_path)

    # 필드 존재 확인
    for col in ["title", "published"]:
        if col not in df.columns:
            raise ValueError(f"Column '{col}' not found in {csv_path}")

    # 1) 제목·본문 텍스트 정제
    df["title"] = df["title"].apply(clean_text)
    if "content" in df.columns:
        df["content"] = df["content"].apply(clean_text)

    # 2) 날짜 표준화
    df["published"] = df["published"].astype(str).apply(normalize_date)

    # 3) 중복 제거
    df = deduplicate(df)

    # 4) 저장
    if save:
        dirname, filename = os.path.split(csv_path)
        cleaned_name = f"clean_{filename}"
        save_path = os.path.join(dirname, cleaned_name)
        df.to_csv(save_path, index=False, encoding="utf-8-sig")
        print(f"[✓] Cleaned CSV saved to {save_path}")

    return df


# ───────────────────────────────────────────────
# 5) 스크립트 단독 실행 예시
# ───────────────────────────────────────────────
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Clean raw news CSV")
    parser.add_argument("csv_path", help="Path to raw news CSV")
    args = parser.parse_args()

    preprocess_news_csv(args.csv_path)
