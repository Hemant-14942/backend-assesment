# Crypto ETL Pipeline Service

A production-ready ETL pipeline built with **FastAPI**, **PostgreSQL**, and **SQLAlchemy**.

## Features
- **Extract**: Fetches data from CoinGecko API and local CSV metadata.
- **Transform**: Merges datasets with case-normalization and handles missing data.
- **Load**: Idempotent UPSERT logic to prevent duplicate records.
- **Reliability**: Exponential backoff retries (3 attempts) and 429 Rate Limit handling.
- **Observability**: Job lineage tracking via the `etl_jobs` table.

## Setup
1. Install dependencies: `uv sync`
2. Configure `.env` with your `DATABASE_URL`.
3. Run the server: `uv run uvicorn app.main:app --reload`