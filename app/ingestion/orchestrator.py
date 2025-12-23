from abc import ABC, abstractmethod
from typing import List, Dict, Any
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from app.services.models import ETLCheckpoint, RawData, CanonicalData
from app.core.logging import logger
from datetime import datetime, timezone
import traceback

class BaseSource(ABC):
    """Abstract Base Class for all Data Sources"""
    
    def __init__(self, source_id: str):
        self.source_id = source_id

    @abstractmethod
    async def fetch_data(self, last_offset: int) -> tuple[List[Dict], int]:
        """Returns (data_batch, new_offset)"""
        pass

    @abstractmethod
    def normalize(self, raw_record: Dict) -> Any:
        """Pure function to convert raw dict to Pydantic Schema"""
        pass

class IngestionOrchestrator:
    def __init__(self, session: AsyncSession, source: BaseSource):
        self.session = session
        self.source = source
        self.log = logger.bind(source=source.source_id)

    async def run(self):
        # 1. Load Checkpoint
        checkpoint = await self.session.get(ETLCheckpoint, self.source.source_id)
        if not checkpoint:
            checkpoint = ETLCheckpoint(source_id=self.source.source_id, last_processed_offset=0)
            self.session.add(checkpoint)
            await self.session.commit()
            
        current_offset = checkpoint.last_processed_offset
        self.log.info("ingestion_start", offset=current_offset)

        try:
            # 2. Fetch Data (Incremental)
            raw_batch, new_offset = await self.source.fetch_data(current_offset)
            
            if not raw_batch:
                self.log.info("no_new_data")
                return

            # 3. Process Batch
            for record in raw_batch:
                # A. Store Raw (EL)
                raw_entry = RawData(source_id=self.source.source_id, payload=record)
                self.session.add(raw_entry)
                
                # B. Normalize & Validate (T)
                try:
                    # Note: normalize now returns a list because one raw record could equal multiple coins, 
                    # but usually it's 1-to-1. We handle the 1-to-1 case here.
                    clean_data_list = self.source.normalize([record])
                    if not clean_data_list:
                        continue
                        
                    clean_data = clean_data_list[0]
                    
                    # C. Load Canonical (Normalization Logic)
                    # FIX: Query by SYMBOL (Unique ID), not by Source+ExternalID
                    stmt = select(CanonicalData).where(
                        CanonicalData.symbol == clean_data.symbol
                    )
                    existing = (await self.session.execute(stmt)).scalar_one_or_none()
                    
                    current_time = datetime.now(timezone.utc)
                    
                    if existing:
                        # UPDATE existing coin (Merge Strategy)
                        existing.price_usd = clean_data.price_usd
                        existing.market_cap = clean_data.market_cap or existing.market_cap
                        existing.last_updated = current_time
                        
                        # Merge provider specific data into JSON
                        # We must create a new dict to ensure SQLAlchemy detects the change
                        current_providers = dict(existing.provider_data) if existing.provider_data else {}
                        current_providers[self.source.source_id] = {
                            "price": clean_data.price_usd,
                            "last_seen": str(current_time)
                        }
                        existing.provider_data = current_providers
                        
                    else:
                        # INSERT new coin
                        new_entry = CanonicalData(
                            symbol=clean_data.symbol,
                            name=clean_data.name,
                            price_usd=clean_data.price_usd,
                            market_cap=clean_data.market_cap,
                            last_updated=current_time,
                            provider_data={
                                self.source.source_id: {
                                    "price": clean_data.price_usd,
                                    "last_seen": str(current_time)
                                }
                            }
                        )
                        self.session.add(new_entry)
                        
                except Exception as e:
                    self.log.error("normalization_error", error=str(e), record=record)
                    continue

            # 4. Update Checkpoint
            checkpoint.last_processed_offset = new_offset
            checkpoint.status = "SUCCESS"
            
            # 5. ATOMIC COMMIT (Raw + Canonical + Checkpoint)
            await self.session.commit()
            self.log.info("ingestion_success", records=len(raw_batch), new_offset=new_offset)

        except Exception as e:
            await self.session.rollback()
            self.log.error("ingestion_failed", error=str(e), trace=traceback.format_exc())
            # Update checkpoint status separately
            checkpoint.status = "FAILED"
            self.session.add(checkpoint)
            await self.session.commit()
            raise e