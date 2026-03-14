from fastapi import APIRouter

router = APIRouter(prefix="/assets", tags=["Assets"])

@router.get("/")
def get_assets():
    return []