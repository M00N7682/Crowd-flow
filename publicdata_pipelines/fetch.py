# fetch.py
import requests

def fetch_weather(api_key, nx, ny, base_date, base_time):
    url = "http://apis.data.go.kr/1360000/VilageFcstInfoService_2.0/getUltraSrtNcst"
    params = {
        "serviceKey": api_key,
        "pageNo": "1",
        "numOfRows": "1000",
        "dataType": "XML",
        "base_date": base_date,
        "base_time": base_time,
        "nx": nx,
        "ny": ny,
    }
    response = requests.get(url, params=params, timeout=10)
    response.raise_for_status()
    return response.text
