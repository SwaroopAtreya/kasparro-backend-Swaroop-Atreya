import httpx
from app.services.models import CanonicalData

class CoinPaprikaSource:
    def __init__(self, source_id: str):
        self.source_id = source_id
        self.base_url = "https://api.coinpaprika.com/v1"
        self.client = httpx.AsyncClient(timeout=10.0)

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

    def normalize(self, raw_data: list[dict]) -> list[CanonicalData]:
        normalized = []
        for item in raw_data:
            try:
                normalized.append(CanonicalData(
                    symbol=item['symbol'].lower(),
                    name=item['name'],
                    price_usd=float(item['quotes']['USD']['price']),
                    source=self.source_id
                ))
            except (KeyError, ValueError) as e:
                print(f"Skipping bad record from {self.source_id}: {e}")
                continue
        return normalized

    async def close(self):
        await self.client.aclose()