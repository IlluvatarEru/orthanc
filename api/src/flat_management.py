"""
Flat management API endpoints.
"""

from datetime import date, datetime
from fastapi import APIRouter, HTTPException, Query
from typing import List, Tuple
import logging

from db.src.write_read_database import OrthancDB

# from db.src.flat_info_from_db import get_flat_info  # Commented out due to import issues
from common.src.flat_info import FlatInfo

logger = logging.getLogger(__name__)
router = APIRouter()


@router.get("/{flat_id}")
async def get_flat_details(flat_id: str):
    """
    Get details for a specific flat.
    """
    try:
        db = OrthancDB()
        flat_info = db.get_flat_info_by_id(flat_id)
        if not flat_info:
            raise HTTPException(status_code=404, detail=f"Flat {flat_id} not found")

        # Convert FlatInfo to dict for JSON serialization
        flat_dict = {
            "flat_id": flat_info.flat_id,
            "price": flat_info.price,
            "area": flat_info.area,
            "flat_type": flat_info.flat_type,
            "residential_complex": flat_info.residential_complex,
            "floor": flat_info.floor,
            "total_floors": flat_info.total_floors,
            "construction_year": flat_info.construction_year,
            "parking": flat_info.parking,
            "description": flat_info.description,
            "is_rental": flat_info.is_rental,
            "url": getattr(flat_info, "url", f"https://krisha.kz/a/show/{flat_id}"),
            "scraped_at": flat_info.scraped_at,
            "published_at": getattr(flat_info, "published_at", None),
            "created_at": getattr(flat_info, "created_at", None),
        }

        return {"success": True, "flat": flat_dict}
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Error getting flat details for {flat_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{flat_id}/similar")
async def get_similar_flats(
    flat_id: str,
    area_tolerance: float = Query(10.0, description="Area tolerance percentage"),
    min_flats: int = Query(3, description="Minimum number of similar flats required"),
):
    """
    Get similar flats for investment analysis.
    """
    try:
        # Get flat info
        db = OrthancDB()
        flat_info = db.get_flat_info_by_id(flat_id)
        if not flat_info:
            raise HTTPException(status_code=404, detail=f"Flat {flat_id} not found")

        # Get similar properties
        similar_rentals, similar_sales = get_similar_properties(
            flat_info, area_tolerance, db=db
        )

        # Check if we have enough data
        if len(similar_sales) < min_flats:
            return {
                "success": False,
                "error": f"Insufficient data for analysis. Found {len(similar_rentals)} rental and {len(similar_sales)} sales flats. Need at least {min_flats} in SALES category.",
                "rental_count": len(similar_rentals),
                "sales_count": len(similar_sales),
                "min_required": min_flats,
            }

        # Convert FlatInfo objects to dict format for JSON serialization
        rental_data = []
        for rental in similar_rentals:
            rental_data.append(
                {
                    "flat_id": rental.flat_id,
                    "price": rental.price,
                    "area": rental.area,
                    "residential_complex": rental.residential_complex,
                    "floor": rental.floor,
                    "construction_year": rental.construction_year,
                }
            )

        sales_data = []
        for sale in similar_sales:
            sales_data.append(
                {
                    "flat_id": sale.flat_id,
                    "price": sale.price,
                    "area": sale.area,
                    "residential_complex": sale.residential_complex,
                    "floor": sale.floor,
                    "construction_year": sale.construction_year,
                }
            )

        return {
            "success": True,
            "flat_info": {
                "flat_id": flat_info.flat_id,
                "price": flat_info.price,
                "area": flat_info.area,
                "residential_complex": flat_info.residential_complex,
                "floor": flat_info.floor,
                "total_floors": flat_info.total_floors,
                "construction_year": flat_info.construction_year,
                "parking": flat_info.parking,
                "description": flat_info.description,
                "is_rental": flat_info.is_rental,
            },
            "similar_rentals": rental_data,
            "similar_sales": sales_data,
            "rental_count": len(rental_data),
            "sales_count": len(sales_data),
            "area_tolerance": area_tolerance,
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Error getting similar flats for {flat_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{flat_id}/market-context")
async def get_market_context(flat_id: str):
    """
    Get market context for a flat: first seen date, days on market, and JK liquidity.
    """
    try:
        db = OrthancDB()
        flat_info = db.get_flat_info_by_id(flat_id)
        if not flat_info:
            raise HTTPException(status_code=404, detail=f"Flat {flat_id} not found")

        first_seen = db.get_flat_first_seen(flat_id)
        days_on_market = None
        if first_seen:
            first_seen_date = datetime.strptime(first_seen, "%Y-%m-%d").date()
            days_on_market = (date.today() - first_seen_date).days

        turnover = {}
        for label, days in [("1m", 30), ("3m", 90), ("6m", 180)]:
            result = db.get_jk_turnover(flat_info.residential_complex, days=days)
            if result:
                turnover[label] = result

        return {
            "success": True,
            "first_seen": first_seen,
            "days_on_market": days_on_market,
            "turnover": turnover,
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Error getting market context for {flat_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


def get_similar_properties(
    flat_info: FlatInfo,
    area_tolerance: float,
    db: OrthancDB = None,
    db_path: str = "flats.db",
) -> Tuple[List[FlatInfo], List[FlatInfo]]:
    """
    Get similar rental and sales properties for analysis.

    :param flat_info: FlatInfo object
    :param area_tolerance: float, area tolerance percentage
    :param db: Optional[OrthancDB], database instance to use (if None, creates new one)
    :param db_path: str, path to database (used only if db is None)
    :return: tuple of (similar_rentals, similar_sales)
    """
    if db is None:
        db = OrthancDB(db_path)

    # Calculate area range
    area_min = flat_info.area * (1 - area_tolerance / 100)
    area_max = flat_info.area * (1 + area_tolerance / 100)

    logger.info(
        f"Searching for flats in complex: {flat_info.residential_complex}, area range: {area_min}-{area_max}"
    )

    # Get similar rentals and sales using database methods (filter by city to avoid cross-city matches)
    similar_rentals = db.get_similar_rentals_by_area_and_complex(
        flat_info.residential_complex, area_min, area_max, city=flat_info.city
    )

    similar_sales = db.get_similar_sales_by_area_and_complex(
        flat_info.residential_complex, area_min, area_max, city=flat_info.city
    )

    logger.info(
        f"Found {len(similar_rentals)} rental flats and {len(similar_sales)} sales flats"
    )

    return similar_rentals, similar_sales
