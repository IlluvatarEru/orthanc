"""
Flat management API endpoints.
"""
from fastapi import APIRouter, HTTPException, Query
from typing import List, Optional, Dict, Tuple
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
        flat_info = _get_flat_info_direct(flat_id)
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
            "url": getattr(flat_info, 'url', f'https://krisha.kz/a/show/{flat_id}')
        }
        
        return {
            "success": True,
            "flat": flat_dict
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting flat details for {flat_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/{flat_id}/similar")
async def get_similar_flats(
    flat_id: str,
    area_tolerance: float = Query(10.0, description="Area tolerance percentage"),
    min_flats: int = Query(3, description="Minimum number of similar flats required")
):
    """
    Get similar flats for investment analysis.
    """
    try:
        # Get flat info
        flat_info = _get_flat_info_direct(flat_id)
        if not flat_info:
            raise HTTPException(status_code=404, detail=f"Flat {flat_id} not found")
        
        # Get similar properties
        similar_rentals, similar_sales = get_similar_properties(flat_info, area_tolerance)
        
        # Check if we have enough data
        if len(similar_rentals) < min_flats or len(similar_sales) < min_flats:
            return {
                "success": False,
                "error": f"Insufficient data for analysis. Found {len(similar_rentals)} rental and {len(similar_sales)} sales flats. Need at least {min_flats} in each category.",
                "rental_count": len(similar_rentals),
                "sales_count": len(similar_sales),
                "min_required": min_flats
            }
        
        # Convert to dict format for JSON serialization
        rental_data = []
        for rental in similar_rentals:
            rental_data.append({
                "flat_id": rental[0],
                "price": rental[1],
                "area": rental[2],
                "residential_complex": rental[3],
                "floor": rental[4],
                "construction_year": rental[5]
            })
        
        sales_data = []
        for sale in similar_sales:
            sales_data.append({
                "flat_id": sale[0],
                "price": sale[1],
                "area": sale[2],
                "residential_complex": sale[3],
                "floor": sale[4],
                "construction_year": sale[5]
            })
        
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
                "is_rental": flat_info.is_rental
            },
            "similar_rentals": rental_data,
            "similar_sales": sales_data,
            "rental_count": len(rental_data),
            "sales_count": len(sales_data),
            "area_tolerance": area_tolerance
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting similar flats for {flat_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

def get_similar_properties(flat_info: FlatInfo, area_tolerance: float, db_path: str = "flats.db") -> Tuple[List, List]:
    """
    Get similar rental and sales properties for analysis.
    
    :param flat_info: FlatInfo object
    :param area_tolerance: float, area tolerance percentage
    :param db_path: str, path to database
    :return: tuple of (similar_rentals, similar_sales)
    """
    db = OrthancDB(db_path)
    try:
        db.connect()

        # Calculate area range
        area_min = flat_info.area * (1 - area_tolerance / 100)
        area_max = flat_info.area * (1 + area_tolerance / 100)

        # Query similar rentals
        jk_arg = f'%{flat_info.residential_complex}%' if flat_info.residential_complex else '%'
        rental_query = f"""
            SELECT DISTINCT flat_id, price, area, residential_complex, floor, construction_year
            FROM rental_flats 
            WHERE residential_complex LIKE '{jk_arg}'
            AND area BETWEEN {area_min} AND {area_max}
            ORDER BY flat_id, query_date DESC
        """
        
        cursor = db.conn.execute(rental_query)
        rental_data = {}
        for row in cursor.fetchall():
            flat_id = row[0]
            if flat_id not in rental_data:
                rental_data[flat_id] = row[1:]

        similar_rentals = list(rental_data.values())

        # Query similar sales
        cursor = db.conn.execute("""
            SELECT DISTINCT flat_id, price, area, residential_complex, floor, construction_year
            FROM sales_flats 
            WHERE residential_complex LIKE ? 
            AND area BETWEEN ? AND ?
            ORDER BY flat_id, query_date DESC
        """, (f'%{flat_info.residential_complex}%' if flat_info.residential_complex else '%', area_min, area_max))

        sales_data = {}
        for row in cursor.fetchall():
            flat_id = row[0]
            if flat_id not in sales_data:
                sales_data[flat_id] = row[1:]

        similar_sales = list(sales_data.values())

        return similar_rentals, similar_sales

    finally:
        db.disconnect()

def _get_flat_info_direct(flat_id: str) -> Optional[FlatInfo]:
    """
    Direct implementation of get_flat_info when the module is not available.
    
    :param flat_id: str, flat ID to get info for
    :return: Optional[FlatInfo], flat information or None if not found
    """
    try:
        # For now, return None as placeholder
        # In a real implementation, this would query the database
        logger.info(f"Getting flat info for {flat_id} (placeholder implementation)")
        return None
    except Exception as e:
        logger.error(f"Error getting flat info for {flat_id}: {e}")
        return None
