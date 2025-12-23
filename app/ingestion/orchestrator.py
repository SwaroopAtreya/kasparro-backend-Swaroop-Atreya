from abc import ABC, abstractmethod
from typing import List, Dict, Any
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from app.services.models import ETLCheckpoint, RawData, CanonicalData
from app.core.logging import logger
from app.schemas.normalized import CanonicalSchema
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
    def normalize(self, raw_record: Dict) -> CanonicalSchema:
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
                    clean_data = self.source.normalize(record)
                    
                    # C. Load Canonical (Upsert logic - handling duplicates)
                    stmt = select(CanonicalData).where(
                        CanonicalData.source == self.source.source_id,
                        CanonicalData.external_id == clean_data.external_id
                    )
                    existing = (await self.session.execute(stmt)).scalar_one_or_none()
                    
                    if existing:
                        # Update fields - UPDATED FOR CRYPTO SCHEMA
                        existing.price_usd = clean_data.price_usd
                        existing.last_updated = clean_data.last_updated
                        existing.market_cap = clean_data.market_cap
                    else:
                        # FIX: Exclude 'source' from the Pydantic dump so we don't pass it twice
                        data_dict = clean_data.model_dump(exclude={'source'})
                        
                        new_entry = CanonicalData(
                            source=self.source.source_id,
                            **data_dict
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