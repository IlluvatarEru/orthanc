from datetime import date

from fastapi import APIRouter, Query

from db.src.write_read_database import OrthancDB

router = APIRouter()


@router.get("/hot")
async def get_hot_jks(limit: int = Query(20, le=50)):
    with OrthancDB() as db:
        rankings = db.get_hot_jks(limit=limit)
    for i, r in enumerate(rankings, 1):
        r["rank"] = i
    week = rankings[0]["week"] if rankings else date.today().strftime("%Y-W%V")
    return {"week": week, "rankings": rankings}


@router.get("/price-trends")
async def get_price_trends():
    with OrthancDB() as db:
        result = db.get_price_trends()
    return result
