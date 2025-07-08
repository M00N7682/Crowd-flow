import xmltodict
import pandas as pd

def parse_weather(xml_text):
    data = xmltodict.parse(xml_text)
    items = data["response"]["body"]["items"]["item"]
    return pd.DataFrame(items)
