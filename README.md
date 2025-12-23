# Kasparro Crypto ETL Platform

A high-performance, asynchronous ETL (Extract, Transform, Load) platform designed to ingest cryptocurrency market data from multiple external APIs, normalize it into a canonical format, and serve it via a REST API.

## 🚀 Features

* **Multi-Source Ingestion:** Simultaneously fetches data from **CoinPaprika** and **CoinGecko**.
* **Incremental Processing:** Tracks "checkpoints" (offsets/pages) in the database to resume ingestion reliably after restarts.
* **Data Normalization:** Converts diverse external API formats into a single, clean `CanonicalSchema`.
* **Asynchronous Architecture:** Built with `FastAPI`, `SQLAlchemy (Async)`, and `asyncio` for non-blocking I/O.
* **Containerized:** Fully Dockerized setup with isolated services for API, Database, and Workers.

## 🛠️ Tech Stack

* **Language:** Python 3.11
* **Framework:** FastAPI
* **Database:** PostgreSQL
* **ORM:** SQLAlchemy (Async)
* **Infrastructure:** Docker & Docker Compose

## 🏃‍♂️ How to Run

1.  **Clone & Configure:**
    ```bash
    cp .env.example .env
    # Add your COINGECKO_API_KEY if available (optional for CoinPaprika)
    ```

2.  **Start Services:**
    ```bash
    docker compose up --build -d
    ```

3.  **Trigger ETL Pipelines:**
    ```bash
    # Trigger CoinPaprika Source
    docker compose exec api python app/trigger_etl.py

    # Trigger CoinGecko Source
    docker compose exec api python app/trigger_coingecko.py
    ```

4.  **Access Data:**
    * **API Stats:** [http://localhost:8000/api/v1/stats](http://localhost:8000/api/v1/stats)
    * **Market Data:** [http://localhost:8000/api/v1/data](http://localhost:8000/api/v1/data)
    * **API Docs:** [http://localhost:8000/docs](http://localhost:8000/docs)

## 🏗️ Architecture

1.  **Extract:** The `BaseSource` abstract class defines a standard interface for fetching data. Implementations (`CoinPaprikaSource`, `CoinGeckoSource`) handle API-specific logic.
2.  **Transform:** Raw data is normalized using Pydantic models (`CanonicalSchema`) to ensure data integrity.
3.  **Load:** The `IngestionOrchestrator` handles the transaction boundaries, saving raw logs (`RawData`) and clean data (`CanonicalData`) atomically.