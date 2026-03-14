import uuid
from sqlalchemy import Column, Integer, String, Float, BigInteger, TIMESTAMP, Text
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.sql import func
from .database import Base

class CryptoAsset(Base):
    __tablename__ = "crypto_assets"

    id = Column(Integer, primary_key=True, index=True) 
    symbol = Column(String, unique=True, index=True, nullable=False)
    name = Column(String)
    price = Column(Float)
    market_cap = Column(BigInteger)
    price_change_24h = Column(Float)
    category = Column(String)
    founding_year = Column(Integer)
    origin_country = Column(String)
    last_updated = Column(TIMESTAMP, server_default=func.now(), onupdate=func.now())

class ETLJob(Base):
    __tablename__ = "etl_jobs"

    job_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    status = Column(String)
    records_processed = Column(Integer, default=0)
    started_at = Column(TIMESTAMP, server_default=func.now())
    finished_at = Column(TIMESTAMP, nullable=True)
    error_message = Column(Text, nullable=True)