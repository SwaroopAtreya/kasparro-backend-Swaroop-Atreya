from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func
from app.core.db import get_db
from app.services.models import CanonicalData, ETLCheckpoint
import time

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