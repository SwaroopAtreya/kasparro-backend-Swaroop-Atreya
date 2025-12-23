import time
from fastapi import APIRouter, Depends, Query, BackgroundTasks
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func
from app.core.db import get_db, AsyncSessionLocal
from app.services.models import CanonicalData, ETLCheckpoint
from app.ingestion.orchestrator import IngestionOrchestrator
from app.ingestion.sources.coinpaprika import CoinPaprikaSource
from app.ingestion.sources.coingecko import CoinGeckoSource

router = APIRouter()

@router.get("/data")
async def get_data(
    page: int = 1,
    limit: int = 20,
    source: str = None,
    db: AsyncSession = Depends(get_db)
):
    start_time = time.time()
    
    query = select(CanonicalData)
    if source:
        query = query.where(CanonicalData.source == source)
    
    query = query.offset((page-1)*limit).limit(limit)
    result = await db.execute(query)
    data = result.scalars().all()
    
    latency = (time.time() - start_time) * 1000
    
    return {
        "metadata": {
            "page": page,
            "limit": limit,
            "latency_ms": round(latency, 2)
        },
        "data": data
    }

@router.get("/stats")
async def get_stats(db: AsyncSession = Depends(get_db)):
    # Aggregation
    count_q = select(func.count(CanonicalData.id))
    total_recs = (await db.execute(count_q)).scalar()
    
    # Checkpoint status
    cp_q = select(ETLCheckpoint)
    checkpoints = (await db.execute(cp_q)).scalars().all()
    
    return {
        "total_records_processed": total_recs,
        "pipelines": [
            {
                "source": cp.source_id,
                "last_run": cp.last_run_timestamp,
                "status": cp.status,
                "offset": cp.last_processed_offset
            } for cp in checkpoints
        ]
    }

# --- NEW TRIGGER LOGIC BELOW ---

async def _run_etl_job(source_name: str):
    """Background task to run ETL without blocking the API"""
    # We use AsyncSessionLocal here because BackgroundTasks run outside the request context
    async with AsyncSessionLocal() as db:
        if source_name == 'coinpaprika':
            source = CoinPaprikaSource('coinpaprika_free')
        elif source_name == 'coingecko':
            source = CoinGeckoSource('coingecko_market')
        else:
            return
        
        orchestrator = IngestionOrchestrator(db, source)
        await orchestrator.run()
        print(f"✅ Manual Trigger: {source_name} finished successfully.")

@router.get("/trigger-etl")
async def trigger_etl(background_tasks: BackgroundTasks, source: str = "coinpaprika"):
    """
    Manually trigger an ETL run.
    Usage: GET /api/v1/trigger-etl?source=coinpaprika
    """
    if source not in ["coinpaprika", "coingecko"]:
        return {"error": "Invalid source. Use 'coinpaprika' or 'coingecko'"}
    
    background_tasks.add_task(_run_etl_job, source)
    return {"message": f"🚀 ETL started for {source}. Check logs or /stats in a few seconds."}