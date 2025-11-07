from pydantic import BaseModel, Field, validator
from datetime import datetime
from typing import Optional, List, Dict, Any
from enum import Enum

class DataLayer(str, Enum):
    BRONZE = 'bronze'
    SILVER = 'silver'
    GOLD = 'gold'

class EventData(BaseModel):
    event_id: str
    event_type: str
    user_id: Optional[str] = None
    timestamp: datetime
    payload: Dict[str, Any]
    source: str = "api"

class Config:
    json_schema = {
        "example":{
            "event_id" : "evt_123",
            "event_type" : "page_view",
            "user_id" : "usr_1234",
            "timestamp" : "2024-01-15T10:30:00",
            "payload" : {"page":"/home"},
            "source" : "web"
        }
    }

class SalesData(BaseModel):
    sale_id: str
    product_id: str
    costumer_id : str
    quantity: int = Field(gt=0)
    price: float = Field(gt=0)
    sale_date: datetime
    region: str

    @validator('price')
    def validate_price(cls, v):
        if v > 1_000_000:
            raise ValueError('Preço muito alto!')
        return round (v,2)

class IngestionResponde(BaseModel):
    status: str
    record_inserted: int
    layer: DataLayer
    table_name: str
    timestamp: datetime

class QueryRequest(BaseModel):
    query: str
    layer: DataLayer = DataLayer.GOLD
    limit: int = Field(default=100, le=10000)
