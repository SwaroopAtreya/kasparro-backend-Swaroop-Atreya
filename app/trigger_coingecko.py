import asyncio
import sys
import os

# Ensure we can import from 'app'
sys.path.append(os.getcwd())

from app.core.db import AsyncSessionLocal
from app.ingestion.orchestrator import IngestionOrchestrator
from app.ingestion.sources.coingecko import CoinGeckoSource

async def main():
    print("--> Connecting to Database...")
    async with AsyncSessionLocal() as db:
        
        # Initialize CoinGecko Source
        # We use a unique source_id 'coingecko_market' so it has its own checkpoint in the DB
        print("--> Initializing CoinGecko Source...")
        source = CoinGeckoSource('coingecko_market')
        
        print("--> Running CoinGecko ETL...")
        orchestrator = IngestionOrchestrator(db, source)
        await orchestrator.run()
        
        print("--> SUCCESS: CoinGecko Data Ingested!")

if __name__ == "__main__":
    asyncio.run(main())