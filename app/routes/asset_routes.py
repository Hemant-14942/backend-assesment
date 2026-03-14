from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.orm import Session

from ..database import get_db
from ..models import CryptoAsset
from ..schemas import CryptoAssetBase

router = APIRouter(prefix="/assets", tags=["Assets"])


@router.get("/", response_model=list[CryptoAssetBase])
def get_assets(
    db: Session = Depends(get_db),
    category: str | None = Query(default=None, description="Filter by category e.g. store_of_value"),
    limit: int | None = Query(default=None, ge=1, description="Limit number of results"),
):
    """
    GET /assets: Return all crypto assets with optional filters.
    Optional: ?category=store_of_value &limit=5
    """
    query = db.query(CryptoAsset).order_by(CryptoAsset.market_cap.desc().nullslast())
    if category is not None:
        query = query.filter(CryptoAsset.category == category)
    if limit is not None:
        query = query.limit(limit)
    assets = query.all()
    return assets


@router.get("/{symbol}", response_model=CryptoAssetBase)
def get_asset_by_symbol(symbol: str, db: Session = Depends(get_db)):
    """
    GET /assets/{symbol}: Return a single asset by symbol (e.g. /assets/btc).
    """
    normalized = symbol.strip().lower()
    asset = db.query(CryptoAsset).filter(CryptoAsset.symbol == normalized).first()
    if asset is None:
        raise HTTPException(status_code=404, detail=f"Asset with symbol '{symbol}' not found")
    return asset
