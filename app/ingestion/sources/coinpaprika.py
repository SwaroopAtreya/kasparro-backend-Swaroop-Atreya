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

    async def fetch_data(self, offset: int) -> tuple[list[dict], int]:
        # CoinPaprika 'tickers' endpoint returns ALL data at once, so we simulate pagination.
        if offset > 0:
            return [], offset

        url = f"{self.base_url}/tickers"
        try:
            response = await self.client.get(url)
            response.raise_for_status()
            data = response.json()
            # Return all data as one batch (limit to first 50 for safety if needed)
            return data[:50], offset + 50
            
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 402:
                print(f"⚠️ Warning: CoinPaprika 402 Payment Required. Skipping source.")
                # Return empty list to signal 'job done' without crashing
                return [], offset 
            raise e  # Re-raise other errors (500, 404, etc)

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