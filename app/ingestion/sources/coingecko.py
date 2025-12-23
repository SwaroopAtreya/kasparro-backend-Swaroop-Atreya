import httpx
from datetime import datetime
from typing import List, Dict
from app.ingestion.orchestrator import BaseSource
from app.schemas.normalized import CanonicalSchema
from app.core.config import settings

class CoinGeckoSource(BaseSource):
    """
    Fetches from CoinGecko API.
    """
    BASE_URL = "https://api.coingecko.com/api/v3"

    async def fetch_data(self, last_offset: int) -> tuple[List[Dict], int]:
        # Pagination: CoinGecko uses pages (1, 2, 3...)
        # We treat 'last_offset' as the page number. Start at 1 if offset is 0.
        page = last_offset + 1
        
        params = {
            "vs_currency": "usd",
            "order": "market_cap_desc",
            "per_page": 20,
            "page": page,
            "sparkline": "false"
        }
        
        # Add API Key if available (prevents 429 errors)
        headers = {}
        if settings.COINGECKO_API_KEY:
            headers["x-cg-demo-api-key"] = settings.COINGECKO_API_KEY

        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(
                    f"{self.BASE_URL}/coins/markets", 
                    params=params,
                    headers=headers
                )
                
                if response.status_code == 429:
                    print("CoinGecko Rate Limit Hit! (Backing off...)")
                    # Return empty batch but do NOT increment offset, so we retry this page next time
                    return [], last_offset 
                
                response.raise_for_status()
                data = response.json()
                
                if not data:
                    return [], last_offset

                # Increment page for next time
                return data, page

        except Exception as e:
            print(f"CoinGecko Error: {e}")
            raise e

    def normalize(self, raw: Dict) -> CanonicalSchema:
        # Normalize fields to match our database schema
        return CanonicalSchema(
            external_id=raw["id"],
            source="coingecko",
            symbol=raw["symbol"].upper(),
            name=raw["name"],
            price_usd=float(raw.get("current_price") or 0),
            market_cap=int(raw.get("market_cap") or 0),
            # CoinGecko uses ISO format with 'Z'
            last_updated=datetime.fromisoformat(raw["last_updated"].replace("Z", "+00:00"))
        )