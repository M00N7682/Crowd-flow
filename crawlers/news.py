import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
from urllib.parse import quote
import sys
import os
from pathlib import Path
import time

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from project_config.loader import load_config

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3"
}

NAVER_SEARCH_URL = "https://search.naver.com/search.naver?where=news&query={query}&sort=1&start={start}&ds={start_date}&de={end_date}"

def build_search_url(query, start_idx, start_date, end_date):
    return NAVER_SEARCH_URL.format(
        query=quote(query),
        start=start_idx,
        start_date=start_date,
        end_date=end_date
    )

def parse_news_page(html):
    soup = BeautifulSoup(html, "html.parser")
    items = soup.select("a[href^='https://'] > span.sds-comps-text-type-headline1")

    news_list = []
    for span_tag in items:
        title = span_tag.get_text(strip=True)
        a_tag = span_tag.find_parent("a")
        url = a_tag.get("href") if a_tag else ""

        if not title or not url:
            continue

        news_list.append({
            "title": title,
            "url": url,
            "source": "",
            "published": ""
        })

    return news_list

def crawl_news_for_keyword(keyword, start_date, end_date, max_pages=5):
    all_results = []
    for page in range(1, max_pages + 1):
        start_idx = (page - 1) * 10 + 1
        url = build_search_url(keyword, start_idx, start_date, end_date)

        try:
            response = requests.get(url, headers=HEADERS, timeout=10)
            response.raise_for_status()
        except Exception as e:
            print(f"[!] Error fetching page {page} for '{keyword}': {e}")
            continue

        page_results = parse_news_page(response.text)
        if not page_results:
            print(f"[-] No more results found on page {page} for '{keyword}'")
            break

        all_results.extend(page_results)
        time.sleep(1)  

    return all_results

def save_news_to_csv(news_data, keyword, output_dir="data/news"):
    os.makedirs(output_dir, exist_ok=True)
    today = datetime.today().strftime("%Y%m%d")
    file_path = os.path.join(output_dir, f"{keyword}_{today}.csv")

    if Path(file_path).exists():
        print(f"[!] Skipping already crawled: {keyword}")
        return

    df = pd.DataFrame(news_data)
    df.to_csv(file_path, index=False, encoding="utf-8-sig")
    print(f"[✓] Saved {len(df)} rows for '{keyword}' to {file_path}")

def run_news_crawler():
    config = load_config()
    start_date = config.date_range.start.strftime("%Y.%m.%d")
    end_date = config.date_range.end.strftime("%Y.%m.%d")

    for keyword in config.keywords:
        print(f"[*] Crawling news for keyword: '{keyword}'")
        results = crawl_news_for_keyword(keyword, start_date, end_date)
        save_news_to_csv(results, keyword)

if __name__ == "__main__":
    run_news_crawler()
