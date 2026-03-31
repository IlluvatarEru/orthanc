"""
JK (Residential Complex) profile endpoint.
"""

import logging
from typing import Optional

from fastapi import APIRouter, HTTPException, Query

from db.src.write_read_database import OrthancDB

logger = logging.getLogger(__name__)
router = APIRouter()

db = OrthancDB("flats.db")


@router.get("/{jk_name}/profile")
async def get_jk_profile(
    jk_name: str,
    flat_type: Optional[str] = Query(None),
    area: Optional[float] = Query(None),
    area_tolerance: Optional[float] = Query(None),
):
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

    # Price trend (all JK flats)
    price_trend = db.get_jk_price_trend(jk_name)

    # Price trend for similar flats (filtered by flat_type and area range)
    price_trend_similar = None
    if flat_type and area and area_tolerance:
        area_min = area * (1 - area_tolerance / 100)
        area_max = area * (1 + area_tolerance / 100)
        price_trend_similar = db.get_jk_price_trend(
            jk_name, flat_type=flat_type, area_min=area_min, area_max=area_max
        )

    # Opportunity frequency
    opp_60 = db.get_opportunity_frequency(jk_name, days=60)
    opp_90 = db.get_opportunity_frequency(jk_name, days=90)

    result = {
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
    if price_trend_similar is not None:
        result["price_trend_similar"] = price_trend_similar
    return result
