import httpx
from datetime import datetime
from typing import List, Dict
from app.ingestion.orchestrator import BaseSource
from app.schemas.normalized import CanonicalSchema

class CoinPaprikaSource(BaseSource):
    """
    Fetches data from CoinPaprika API (Public Free Tier).
    Docs: https://api.coinpaprika.com/
    """
    # Note: The free public API does not use /v1/tickers in the same way with keys.
    # We use the public endpoint.
    BASE_URL = "https://api.coinpaprika.com/v1"

    async def fetch_data(self, last_offset: int) -> tuple[List[Dict], int]:
        # For the anonymous free tier, we simply make a GET request without headers.
        
        async with httpx.AsyncClient() as client:
            # We fetch all tickers. The free tier allows this.
            response = await client.get(f"{self.BASE_URL}/tickers")
            
            if response.status_code == 429:
                print("CoinPaprika Rate Limit! Cooling down...")
                return [], last_offset

            response.raise_for_status()
            all_data = response.json()

        # Incremental Simulation:
        # We slice the big list of results into small batches (e.g., 50 at a time)
        # based on the 'last_offset'.
        batch_size = 50
        batch = all_data[last_offset : last_offset + batch_size]
        
        # If the batch is empty (we reached the end of the list), return empty.
        if not batch:
            return [], last_offset

        return batch, last_offset + len(batch)

    def normalize(self, raw: Dict) -> CanonicalSchema:
        # Map raw API data to our clean database schema
        return CanonicalSchema(
            external_id=raw["id"],
            source="coinpaprika",
            symbol=raw["symbol"],
            name=raw["name"],
            # The tickers endpoint in free tier puts price inside "quotes" -> "USD"
            price_usd=float(raw.get("quotes", {}).get("USD", {}).get("price", 0)),
            market_cap=int(raw.get("quotes", {}).get("USD", {}).get("market_cap", 0)),
            last_updated=datetime.now() # Use current time as ingestion time
        )