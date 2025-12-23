import asyncio
import sys
import os

# Ensure we can import from 'app'
sys.path.append(os.getcwd())

from app.core.db import AsyncSessionLocal, engine, Base
from app.ingestion.orchestrator import IngestionOrchestrator
from app.ingestion.sources.coinpaprika import CoinPaprikaSource
# Import models to ensure they are registered with Base.metadata
from app.services.models import ETLCheckpoint, RawData, CanonicalData

async def init_db():
    print("--> Checking Database Schema...")
    async with engine.begin() as conn:
        # This forces the creation of tables if they don't exist
        await conn.run_sync(Base.metadata.create_all)
    print("--> Database Tables Verified/Created.")

async def main():
    # 1. Ensure DB exists
    await init_db()

    print("--> Connecting to Database...")
    async with AsyncSessionLocal() as db:
        
        # 2. Initialize the Source
        print("--> Initializing CoinPaprika Source...")
        source = CoinPaprikaSource('coinpaprika_free')
        
        # 3. Run the Orchestrator
        print("--> Running ETL Pipeline...")
        orchestrator = IngestionOrchestrator(db, source)
        await orchestrator.run()
        
        print("--> SUCCESS: ETL Pipeline Finished!")

if __name__ == "__main__":
    asyncio.run(main())