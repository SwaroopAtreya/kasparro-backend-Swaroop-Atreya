from pydantic import BaseModel
from datetime import datetime
from typing import Optional

class CanonicalSchema(BaseModel):
    external_id: str
    source: str
    symbol: str
    name: str
    price_usd: float
    market_cap: int
    last_updated: datetime
    
    # We removed user_name, email, transaction_amount