import pytest
import asyncio
from sqlalchemy import text
from app.core.db import engine, Base, AsyncSessionLocal

# 1. FIX THE LOOP ERROR: Force one event loop for the whole test session
@pytest.fixture(scope="session")
def event_loop():
    """Overrides pytest default loop to be session-scoped."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()

# 2. FIX THE DIRTY DB: Wipe database before every test function
@pytest.fixture(scope="function", autouse=True)
async def setup_db():
    """Cleans the database before each test."""
    async with engine.begin() as conn:
        # Create tables if missing
        await conn.run_sync(Base.metadata.create_all)
        
        # TRUNCATE all tables to ensure a clean slate
        # We restart identity to reset ID counters to 1
        await conn.execute(text("TRUNCATE TABLE canonical_data, etl_checkpoints RESTART IDENTITY CASCADE"))
    
    yield
    
    # Optional: cleanup after test (not strictly needed if we clean before)