from fastapi import FastAPI
from app.api.routes import data
from app.core.logging import setup_logging
from contextlib import asynccontextmanager
from prometheus_client import make_asgi_app
from app.core.db import engine, Base

# IMPORTANT: Import models here so SQLAlchemy knows they exist
# If you don't import them, the tables won't be created!
from app.services.models import ETLCheckpoint, RawData, CanonicalData

setup_logging()

@asynccontextmanager
async def lifespan(app: FastAPI):
    # STARTUP: Create all tables defined in models
    print("Creating database tables...")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    print("Database tables created.")
    
    yield
    # SHUTDOWN: Cleanup if needed

app = FastAPI(title="Kasparro Evaluation Platform", lifespan=lifespan)

# Observability
metrics_app = make_asgi_app()
app.mount("/metrics", metrics_app)

app.include_router(data.router, prefix="/api/v1")

@app.get("/health")
async def health_check():
    return {"status": "ok", "db": "connected"}