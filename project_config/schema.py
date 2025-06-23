from pydantic import BaseModel, Field
from typing import List, Literal
from datetime import date

class DateRange(BaseModel):
    start: date
    end: date

class ClientConfig(BaseModel):
    industry: str
    keywords: List[str]
    sources: List[Literal["news", "publicdata", "forum"]]
    date_range: DateRange
    output_format: Literal["csv", "db"]
    language: Literal["ko", "en"] = "ko"
