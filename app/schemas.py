from pydantic import BaseModel
from datetime import datetime
from typing import Optional, List
from uuid import UUID

# Schema for the individual Asset (Requirement: 68)
class CryptoAssetBase(BaseModel):
    symbol: str
    name: str
    price: float
    market_cap: int
    price_change_24h: float
    category: Optional[str] = None
    founding_year: Optional[int] = None
    origin_country: Optional[str] = None
    last_updated: datetime

    class Config:
        from_attributes = True

# Schema for the ETL Job status (Requirement: 144)
class ETLJobSchema(BaseModel):
    job_id: UUID
    status: str
    records_processed: int
    started_at: datetime
    finished_at: Optional[datetime] = None
    error_message: Optional[str] = None

    class Config:
        from_attributes = True