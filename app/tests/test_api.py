import pytest
from httpx import AsyncClient, ASGITransport
from app.main import app
from app.services.models import CanonicalData, ETLCheckpoint
from app.core.db import AsyncSessionLocal
from sqlalchemy import text
from datetime import datetime

# Mark all tests as asyncio
pytestmark = pytest.mark.asyncio

async def seed_and_clean_data():
    """Wipes the DB and inserts exactly one mock record."""
    async with AsyncSessionLocal() as session:
        # 1. NUCLEAR CLEANUP: Truncate tables to ensure database is empty
        # 'CASCADE' ensures linked tables (if any) are also handled
        await session.execute(text("TRUNCATE TABLE canonical_data, etl_checkpoints RESTART IDENTITY CASCADE"))
        
        # 2. Insert Mock Checkpoint
        checkpoint = ETLCheckpoint(
            source_id="test_source_1",
            last_processed_offset=50,
            status="SUCCESS",
            last_run_timestamp=datetime.now()
        )
        session.add(checkpoint)

        # 3. Insert Mock Crypto Data
        crypto_entry = CanonicalData(
            external_id="test-btc",
            source="test_source_1",
            symbol="BTC",
            name="Bitcoin Test",
            price_usd=50000.00,
            market_cap=1000000000,
            last_updated=datetime.now()
        )
        session.add(crypto_entry)
        await session.commit()

async def test_read_market_data():
    # 1. Prepare DB (Clean + Seed)
    await seed_and_clean_data()

    # 2. Request
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        response = await ac.get("/api/v1/data")
    
    # 3. Validate
    assert response.status_code == 200
    json_data = response.json()
    
    assert "data" in json_data
    # This will now PASS because we ran TRUNCATE right before
    assert len(json_data["data"]) == 1 
    assert json_data["data"][0]["symbol"] == "BTC"

async def test_read_stats():
    # 1. Prepare DB (Clean + Seed)
    await seed_and_clean_data()

    # 2. Request
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        response = await ac.get("/api/v1/stats")
    
    # 3. Validate
    assert response.status_code == 200
    stats = response.json()
    
    assert stats["total_records_processed"] == 1
    assert stats["pipelines"][0]["source"] == "test_source_1"