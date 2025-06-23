import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime, timedelta
from urllib.parse import quote
from config.loader import load_config
import os

HEADERS = {
    "User-Agent": "Mozilla/5.0"
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
    items = soup.select(".list_news > li")

    news_list = []
    for item in items:
        title_tag = item.select_one("a.news_tit")
        if not title_tag:
            continue
        title = title_tag.get("title")
        url = title_tag.get("href")

        source_tag = item.select_one(".info.press")
        source = source_tag.get_text(strip=True) if source_tag else ""

        date_tag = item.select_one(".info_group > span.info")
        pub_date = date_tag.get_text(strip=True) if date_tag else ""

        news_list.append({
            "title": title,
            "url": url,
            "source": source,
            "published": pub_date
        })

    return news_list

def crawl_news_for_keyword(keyword, start_date, end_date, max_pages=5):
    all_results = []
    for page in range(1, max_pages + 1):
        start_idx = (page - 1) * 10 + 1
        url = build_search_url(keyword, start_idx, start_date, end_date)
        res = requests.get(url, headers=HEADERS)

        if res.status_code != 200:
            print(f"Failed to fetch page {page} for '{keyword}'")
            continue

        page_results = parse_news_page(res.text)
        if not page_results:
            break

        all_results.extend(page_results)
    return all_results

def save_news_to_csv(news_data, keyword, output_dir="data/news"):
    os.makedirs(output_dir, exist_ok=True)
    df = pd.DataFrame(news_data)
    today = datetime.today().strftime("%Y%m%d")
    file_path = os.path.join(output_dir, f"{keyword}_{today}.csv")
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
