from sqlalchemy import Column, Integer, String, DateTime, JSON, Float, BigInteger, Index
from sqlalchemy.sql import func
from app.core.db import Base

class ETLCheckpoint(Base):
    __tablename__ = "etl_checkpoints"
    source_id = Column(String, primary_key=True)
    last_processed_offset = Column(BigInteger, default=0)
    last_run_timestamp = Column(DateTime(timezone=True), default=func.now())
    status = Column(String, default="SUCCESS")
    metadata_blob = Column(JSON, default={})

class RawData(Base):
    __tablename__ = "raw_data"
    id = Column(Integer, primary_key=True, index=True)
    source_id = Column(String, index=True)
    payload = Column(JSON)
    ingested_at = Column(DateTime(timezone=True), server_default=func.now())

class CanonicalData(Base):
    __tablename__ = "canonical_data"
    
    id = Column(Integer, primary_key=True, index=True)
    external_id = Column(String, index=True)
    source = Column(String)
    
    # CRYPTO FIELDS (Make sure these are here!)
    symbol = Column(String, index=True)
    name = Column(String)
    price_usd = Column(Float)
    market_cap = Column(BigInteger)
    last_updated = Column(DateTime(timezone=True))
    
    processed_at = Column(DateTime(timezone=True), server_default=func.now())
    
    __table_args__ = (
        Index('idx_source_external', 'source', 'external_id', unique=False),
    )