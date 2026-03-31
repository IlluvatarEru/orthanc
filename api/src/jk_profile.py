"""
JK (Residential Complex) profile endpoint.
"""

import logging
from fastapi import APIRouter, HTTPException

from db.src.write_read_database import OrthancDB

logger = logging.getLogger(__name__)
router = APIRouter()

db = OrthancDB("flats.db")


@router.get("/{jk_name}/profile")
async def get_jk_profile(jk_name: str):
    """Get comprehensive profile for a residential complex."""
    # Basic info
    rc = db.get_residential_complex_by_name(jk_name)
    if not rc:
        raise HTTPException(status_code=404, detail=f"JK '{jk_name}' not found")

    # Sales stats from latest snapshot
    snapshots = db.get_jk_performance_snapshots(
        residential_complex=jk_name,
    )
    sales = {}
    if snapshots:
        snap = snapshots[0]  # latest
        sales = {
            "count": snap.get("total_sales_flats", 0),
            "median_price": snap.get("median_sales_price_per_m2"),
            "avg_price_per_sqm": snap.get("mean_sales_price_per_m2"),
            "yield_min": snap.get("min_rental_yield"),
            "yield_max": snap.get("max_rental_yield"),
            "yield_median": snap.get("median_rental_yield"),
        }

    # Turnover
    turnover = {}
    for label, days in [("1m", 30), ("3m", 90), ("6m", 180)]:
        t = db.get_jk_turnover(jk_name, days=days)
        if t:
            turnover[label] = round(t["turnover_pct"] / 100, 3)
        else:
            turnover[label] = None

    # Price trend
    price_trend = db.get_jk_price_trend(jk_name)

    # Opportunity frequency
    opp_60 = db.get_opportunity_frequency(jk_name, days=60)
    opp_90 = db.get_opportunity_frequency(jk_name, days=90)

    return {
        "success": True,
        "name": rc["name"],
        "city": rc.get("city"),
        "district": rc.get("district"),
        "sales": sales,
        "turnover": turnover,
        "price_trend": price_trend,
        "opportunity_frequency": {
            "last_60d": opp_60,
            "last_90d": opp_90,
        },
    }
