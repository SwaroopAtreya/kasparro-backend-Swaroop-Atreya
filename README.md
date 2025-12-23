# Kasparro Crypto ETL Platform

A high-performance, asynchronous ETL (Extract, Transform, Load) platform designed to ingest cryptocurrency market data from multiple external APIs (CoinGecko, CoinPaprika), normalize it into a canonical schema, and serve it via a high-speed REST API.

Built with **FastAPI**, **SQLAlchemy (Async)**, and **Docker**.

---

## 🌍 Live Deployment (Cloud)

The project is deployed on **Render** (Cloud Provider). You can verify the running system here:

* **API Base URL:** [https://kasparro-backend-swaroop-atreya.onrender.com](https://kasparro-backend-swaroop-atreya.onrender.com)
* **📊 Pipeline Stats (Output):** [https://kasparro-backend-swaroop-atreya.onrender.com/api/v1/stats](https://kasparro-backend-swaroop-atreya.onrender.com/api/v1/stats)
    * *Check this to see total records processed and pipeline success status.*
* **💰 Market Data (Output):** [https://kasparro-backend-swaroop-atreya.onrender.com/api/v1/data](https://kasparro-backend-swaroop-atreya.onrender.com/api/v1/data)
    * *Live JSON data showing normalized Crypto prices.*
* **Documentation:** [https://kasparro-backend-swaroop-atreya.onrender.com/docs](https://kasparro-backend-swaroop-atreya.onrender.com/docs)

---

## 🚀 Features Implemented

### ✅ P0: Foundation Layer
* **Multi-Source Ingestion:** Fetches data from **CoinPaprika** and **CoinGecko**.
* **Data Normalization:** Maps diverse source APIs into a unified `CanonicalData` table.
* **Dockerized:** Full `docker-compose` setup for local development.

### ⭐ P1: Growth Layer
* **Incremental Ingestion:** Uses an `etl_checkpoints` table to track offsets and resume capability.
* **Stats Endpoint:** Tracks health, last run times, and success rates per pipeline.
* **Clean Architecture:** Separation of concerns (`ingestion`, `services`, `api`, `core`).

### 🚀 P2: Differentiator Layer
* **Cloud Trigger Endpoint:** A dedicated endpoint to manually trigger ETL runs in the cloud (enables usage with external Cron services).
* **Rate Limiting Handling:** Intelligent backoff logic for API limits.
* **Structured Logging:** JSON-based logs for observability.

---

## 🛠️ How to Run Locally

If you wish to run the project on your local machine using Docker:

1.  **Clone the repository:**
    ```bash
    git clone [https://github.com/SwaroopAtreya/kasparro-backend-Swaroop-Atreya.git](https://github.com/SwaroopAtreya/kasparro-backend-Swaroop-Atreya.git)
    cd kasparro-backend-Swaroop-Atreya
    ```

2.  **Configure Environment:**
    Create a `.env` file in the root directory:
    ```ini
    PROJECT_NAME="Kasparro ETL"
    POSTGRES_SERVER=db
    POSTGRES_USER=postgres
    POSTGRES_PASSWORD=secretpassword
    POSTGRES_DB=kasparro_db
    # Optional: Add your real API keys if available
    COINGECKO_API_KEY=
    ```

3.  **Start Services:**
    ```bash
    docker compose up --build -d
    ```

4.  **Trigger ETL Pipelines (Locally):**
    ```bash
    # Run CoinPaprika Pipeline
    docker compose exec api python app/trigger_etl.py
    
    # Run CoinGecko Pipeline
    docker compose exec api python app/trigger_coingecko.py
    ```

5.  **Run Tests:**
    ```bash
    docker compose exec api pytest
    ```

---

## ☁️ Cloud Architecture & Scheduling

Since the cloud deployment uses a free-tier hosting plan (Render), direct shell access for cron jobs is restricted. To satisfy the **Cloud Scheduler** requirement, I implemented a **REST-based Trigger System**.

### How to Trigger ETL in Cloud
Instead of a server-side cron, the system exposes a secure endpoint that can be pinged by any external scheduler (like `cron-job.org` or GitHub Actions).

**Trigger Endpoints:**
* **CoinPaprika:** `GET /api/v1/trigger-etl?source=coinpaprika`
* **CoinGecko:** `GET /api/v1/trigger-etl?source=coingecko`

*Note: In a production environment with a paid budget, these endpoints would be called by a native Render Cron Job service.*

---

## 🔎 Verification & Output

### 1. Check Pipeline Health
Visit `/api/v1/stats` to see the ETL summary.
**Expected Output:**
```json
{
  "total_records_processed": 120,
  "pipelines": [
    {
      "source": "coinpaprika_free",
      "status": "SUCCESS",
      "offset": 100
    },
    {
      "source": "coingecko_market",
      "status": "SUCCESS",
      "offset": 1
    }
  ]
}
2. Check Data Integrity
Visit /api/v1/data to see the normalized data. Expected Output:

JSON

{
  "data": [
    {
      "symbol": "btc",
      "name": "Bitcoin",
      "price_usd": 97234.50,
      "source": "coinpaprika_free"
    },
    ...
  ]
}


🛠️ Tech Stack
Language: Python 3.11

Web Framework: FastAPI

Database: PostgreSQL

ORM: SQLAlchemy (Async)

Containerization: Docker & Docker Compose

Deployment: Render Cloud
