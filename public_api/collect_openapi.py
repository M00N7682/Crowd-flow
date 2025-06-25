import requests
import pandas as pd
import os
from datetime import datetime
from pathlib import Path

API_KEY = "발급받은_인증키"
BASE_URL = "http://apis.data.go.kr/B552584/ArpltnInforInqireSvc/getCtprvnRltmMesureDnsty"

PARAMS = {
    "serviceKey": API_KEY,
    "returnType": "json",
    "numOfRows": 100,
    "pageNo": 1,
    "sidoName": "서울",
    "ver": "1.0"
}

def fetch_air_quality_data():
    response = requests.get(BASE_URL, params=PARAMS)
    data = response.json()

    items = data.get("response", {}).get("body", {}).get("items", [])
    df = pd.DataFrame(items)

    return df

def save_to_csv(df, output_dir="data/public"):
    os.makedirs(output_dir, exist_ok=True)
    filename = f"air_quality_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    df.to_csv(Path(output_dir) / filename, index=False, encoding="utf-8-sig")
    print(f"[✓] 저장 완료: {filename}")

if __name__ == "__main__":
    df = fetch_air_quality_data()
    if not df.empty:
        save_to_csv(df)
    else:
        print("[!] 수집된 데이터가 없습니다.")
