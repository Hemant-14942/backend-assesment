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
CSV_PATH = Path(__file__).resolve().parent.parent / "data" / "crypto_metadata.csv"

@retry_with_backoff(retries=3)
def fetch_api_data():
    """EXTRACT: Fetch data from Public API (Requirement: Source 1)"""
    response = requests.get(COINGECKO_URL)
    response.raise_for_status() 
    return response.json()

def run_etl(db: Session, job_id: str, use_mock: bool = False):
    """The main ETL Pipeline execution logic with a demo mock toggle"""
    try:
        # 1. EXTRACT 
        if use_mock:
            logger.info("Manual Mock Triggered: Bypassing CoinGecko API for demo")
            api_data = [
                {"symbol": "btc", "name": "Bitcoin", "current_price": 65000.0, "market_cap": 1200000000000, "price_change_percentage_24h": -1.5},
                {"symbol": "eth", "name": "Ethereum", "current_price": 3500.0, "market_cap": 400000000000, "price_change_percentage_24h": 2.1},
                {"symbol": "bnb", "name": "BNB", "current_price": 580.0, "market_cap": 88000000000, "price_change_percentage_24h": 0.8},
                {"symbol": "sol", "name": "Solana", "current_price": 145.0, "market_cap": 65000000000, "price_change_percentage_24h": 3.2},
            ]
        else:
            api_data = fetch_api_data() 
            
        df_metadata = pd.read_csv(str(CSV_PATH))

        # 2. TRANSFORM 
        df_api = pd.DataFrame(api_data)
        
        # Normalize fields (Requirement: Case Mismatch) 
        df_api['symbol'] = df_api['symbol'].str.lower()
        df_metadata['symbol'] = df_metadata['symbol'].str.lower()

        # Merge datasets using 'symbol' 
        merged_df = pd.merge(df_api, df_metadata, on='symbol', how='left')

        # 3. LOAD (Idempotent UPSERT)
        records_count = 0
        for _, row in merged_df.iterrows():
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

            # Log each record being processed for observability 
            logger.info(f"Processing UPSERT for symbol: {data['symbol']}")

            stmt = insert(CryptoAsset).values(data)
            stmt = stmt.on_conflict_do_update(
                index_elements=['symbol'], 
                set_=data
            )
            db.execute(stmt)
            records_count += 1
        
        db.commit()
        return records_count, None

    except Exception as e:
        db.rollback()
        # Logging of ETL failures 
        logger.error(f"ETL pipeline job {job_id} failed: {str(e)}")
        return 0, str(e)