import logging
from pathlib import Path

import pandas as pd
import requests
from sqlalchemy.orm import Session
from sqlalchemy.dialects.postgresql import insert
from datetime import datetime

from .models import CryptoAsset, ETLJob
from .retry_utils import retry_with_backoff

logger = logging.getLogger(__name__)

# Source URLs from assignment
COINGECKO_URL = "https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&order=market_cap_desc&per_page=10&page=1"
# Path relative to project root (robust regardless of cwd)
CSV_PATH = Path(__file__).resolve().parent.parent / "data" / "crypto_metadata.csv"

@retry_with_backoff(retries=3)
def fetch_api_data():
    """EXTRACT: Fetch data from Public API (Requirement: Source 1)"""
    response = requests.get(COINGECKO_URL)
    response.raise_for_status() # Raises error for 4xx/5xx to trigger retry
    return response.json()

def run_etl(db: Session, job_id: str):
    """The main ETL Pipeline execution logic"""
    try:
        # 1. EXTRACT
        api_data = fetch_api_data()
        df_metadata = pd.read_csv(str(CSV_PATH))

        # 2. TRANSFORM
        # Convert API list to DataFrame
        df_api = pd.DataFrame(api_data)
        
        # Normalize: Handle Case Mismatch (BTC vs btc) - Requirement: Case Mismatch
        df_api['symbol'] = df_api['symbol'].str.lower()
        df_metadata['symbol'] = df_metadata['symbol'].str.lower()

        # Merge datasets using 'symbol' - Requirement: Merge datasets
        # 'left' join ensures we keep all API data even if metadata is missing
        merged_df = pd.merge(df_api, df_metadata, on='symbol', how='left')

        # 3. LOAD (with Idempotency/UPSERT) - Requirement: 1. Idempotent ETL
        records_count = 0
        for _, row in merged_df.iterrows():
            # Prepare the data dictionary
            data = {
                "symbol": row['symbol'],
                "name": row['name'],
                "price": row['current_price'],
                "market_cap": row['market_cap'],
                "price_change_24h": row['price_change_percentage_24h'],
                "category": row.get('category'),
                "founding_year": int(row["founding_year"]) if pd.notnull(row.get("founding_year")) else None,
                "origin_country": row.get('origin_country'),
                "last_updated": datetime.utcnow()
            }

            # PostgreSQL UPSERT logic: Insert or Update on Conflict
            stmt = insert(CryptoAsset).values(data)
            stmt = stmt.on_conflict_do_update(
                index_elements=['symbol'], # Requirement: ON CONFLICT (symbol)
                set_=data
            )
            db.execute(stmt)
            records_count += 1
        
        db.commit()
        return records_count, None

    except Exception as e:
        db.rollback()
        logger.exception("ETL pipeline failed: %s", e)
        return 0, str(e)