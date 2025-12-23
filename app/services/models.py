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
    
    # NORMALIZATION FIX:
    # We normalize based on 'symbol'. Unique constraint ensures one row per coin (e.g. 'btc').
    symbol = Column(String, unique=True, index=True, nullable=False)
    
    name = Column(String, nullable=False)
    price_usd = Column(Float, nullable=False)
    market_cap = Column(BigInteger, nullable=True)
    
    # Store source-specific data here (e.g. {"coingecko": {"price": 500}, "coinpaprika": {"price": 501}})
    provider_data = Column(JSON, default=dict)
    
    last_updated = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
    processed_at = Column(DateTime(timezone=True), server_default=func.now())

    # No composite index on source+external_id anymore. 
    # Normalization is enforced by the unique symbol constraint.