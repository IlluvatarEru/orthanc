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
        logger.exception(f"Error getting flat details for {flat_id}: {e}")
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
        
        # Convert FlatInfo objects to dict format for JSON serialization
        rental_data = []
        for rental in similar_rentals:
            rental_data.append({
                "flat_id": rental.flat_id,
                "price": rental.price,
                "area": rental.area,
                "residential_complex": rental.residential_complex,
                "floor": rental.floor,
                "construction_year": rental.construction_year
            })
        
        sales_data = []
        for sale in similar_sales:
            sales_data.append({
                "flat_id": sale.flat_id,
                "price": sale.price,
                "area": sale.area,
                "residential_complex": sale.residential_complex,
                "floor": sale.floor,
                "construction_year": sale.construction_year
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
        logger.exception(f"Error getting similar flats for {flat_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

def get_similar_properties(flat_info: FlatInfo, area_tolerance: float, db_path: str = "flats.db") -> Tuple[List[FlatInfo], List[FlatInfo]]:
    """
    Get similar rental and sales properties for analysis.
    
    :param flat_info: FlatInfo object
    :param area_tolerance: float, area tolerance percentage
    :param db_path: str, path to database
    :return: tuple of (similar_rentals, similar_sales)
    """
    db = OrthancDB(db_path)
    db.connect()

    # Calculate area range
    area_min = flat_info.area * (1 - area_tolerance / 100)
    area_max = flat_info.area * (1 + area_tolerance / 100)

    # Query similar rentals
    jk_arg = f'%{flat_info.residential_complex}%' if flat_info.residential_complex else '%'
    cursor = db.conn.execute("""
        SELECT DISTINCT flat_id, price, area, residential_complex, floor, construction_year
        FROM rental_flats 
        WHERE residential_complex LIKE ? 
        AND area BETWEEN ? AND ?
        ORDER BY flat_id, query_date DESC
    """, (jk_arg, area_min, area_max))

    logger.info(f"Searching for flats in complex: {flat_info.residential_complex}, area range: {area_min}-{area_max}")

    similar_rentals = []
    rows = cursor.fetchall()
    logger.info(f"Found {len(rows)} rental rows")

    for i, row in enumerate(rows):
        logger.info(f"Processing rental row {i}: length={len(row)}, row={row}")
        # Create FlatInfo object from row data
        rental_flat = FlatInfo(
            flat_id=row[0],
            price=row[1],
            area=row[2],
            flat_type=row[3],  # residential_complex from query
            residential_complex=row[3],
            floor=row[4],
            total_floors=0,  # Not in query, use default
            construction_year=row[5],
            parking=False,  # Not in query, use default
            description="",  # Not in query, use default
            is_rental=True
        )
        similar_rentals.append(rental_flat)

    # Query similar sales
    cursor = db.conn.execute("""
        SELECT DISTINCT flat_id, price, area, residential_complex, floor, construction_year
        FROM sales_flats 
        WHERE residential_complex LIKE ? 
        AND area BETWEEN ? AND ?
        ORDER BY flat_id, query_date DESC
    """, (f'%{flat_info.residential_complex}%' if flat_info.residential_complex else '%', area_min, area_max))

    logger.info(f"Searching for sales flats in complex: {flat_info.residential_complex}, area range: {area_min}-{area_max}")

    similar_sales = []
    sales_rows = cursor.fetchall()
    logger.info(f"Found {len(sales_rows)} sales rows")

    for i, row in enumerate(sales_rows):
        logger.info(f"Processing sales row {i}: length={len(row)}, row={row}")
        # Create FlatInfo object from row data
        sales_flat = FlatInfo(
            flat_id=row[0],
            price=row[1],
            area=row[2],
            flat_type=row[3],  # residential_complex from query
            residential_complex=row[3],
            floor=row[4],
            total_floors=0,  # Not in query, use default
            construction_year=row[5],
            parking=False,  # Not in query, use default
            description="",  # Not in query, use default
            is_rental=False
        )
        similar_sales.append(sales_flat)


    logger.info(f"Returning {len(similar_rentals)} rental flats and {len(similar_sales)} sales flats")

    return similar_rentals, similar_sales

def _get_flat_info_direct(flat_id: str) -> Optional[FlatInfo]:
    """
    Direct implementation of get_flat_info when the module is not available.
    
    :param flat_id: str, flat ID to get info for
    :return: Optional[FlatInfo], flat information or None if not found
    """
    db = OrthancDB()
    db.connect()
    
    # Try to find the flat in rental_flats first
    cursor = db.conn.execute("""
        SELECT flat_id, price, area, flat_type, residential_complex, floor, 
               total_floors, construction_year, parking, description, url, query_date
        FROM rental_flats 
        WHERE flat_id = ?
        ORDER BY query_date DESC
        LIMIT 1
    """, (flat_id,))
    
    row = cursor.fetchone()
    if row:
        flat_info = FlatInfo(
            flat_id=row[0],
            price=row[1],
            area=row[2],
            flat_type=row[3],
            residential_complex=row[4],
            floor=row[5],
            total_floors=row[6],
            construction_year=row[7],
            parking=row[8],
            description=row[9],
            is_rental=True
        )
        flat_info.url = row[10] if row[10] else f'https://krisha.kz/a/show/{flat_id}'
        db.disconnect()
        return flat_info
    
    # If not found in rental_flats, try sales_flats
    cursor = db.conn.execute("""
        SELECT flat_id, price, area, flat_type, residential_complex, floor, 
               total_floors, construction_year, parking, description, url, query_date
        FROM sales_flats 
        WHERE flat_id = ?
        ORDER BY query_date DESC
        LIMIT 1
    """, (flat_id,))
    
    row = cursor.fetchone()
    if row:
        flat_info = FlatInfo(
            flat_id=row[0],
            price=row[1],
            area=row[2],
            flat_type=row[3],
            residential_complex=row[4],
            floor=row[5],
            total_floors=row[6],
            construction_year=row[7],
            parking=row[8],
            description=row[9],
            is_rental=False
        )
        flat_info.url = row[10] if row[10] else f'https://krisha.kz/a/show/{flat_id}'
        db.disconnect()
        return flat_info
    
    db.disconnect()
    return None