"""
Flat-specific analysis endpoints.
"""
from fastapi import APIRouter, HTTPException, Query
from typing import List, Optional
import logging
import sys
import os

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from common.src.flat_info import FlatInfo
from db.src.write_read_database import OrthancDB

logger = logging.getLogger(__name__)
router = APIRouter()

@router.get("/search")
async def search_flats(
    flat_type: Optional[str] = Query(None, description="Filter by flat type (1BR, 2BR, 3BR, etc.)"),
    min_price: Optional[int] = Query(None, description="Minimum price filter"),
    max_price: Optional[int] = Query(None, description="Maximum price filter"),
    min_area: Optional[float] = Query(None, description="Minimum area filter"),
    max_area: Optional[float] = Query(None, description="Maximum area filter"),
    residential_complex: Optional[str] = Query(None, description="Filter by residential complex name"),
    is_rental: Optional[bool] = Query(None, description="Filter by rental status"),
    limit: int = Query(50, description="Maximum number of results")
):
    """
    Search for flats with various filters.
    """
    try:
        db = OrthancDB("flats.db")
        db.connect()
        
        # Build query based on filters
        query = "SELECT * FROM ("
        query += "SELECT *, 'rental' as table_type FROM rental_flats "
        query += "UNION ALL "
        query += "SELECT *, 'sale' as table_type FROM sales_flats"
        query += ") WHERE 1=1"
        
        params = []
        
        if flat_type:
            query += " AND flat_type = ?"
            params.append(flat_type)
            
        if min_price:
            query += " AND price >= ?"
            params.append(min_price)
            
        if max_price:
            query += " AND price <= ?"
            params.append(max_price)
            
        if min_area:
            query += " AND area >= ?"
            params.append(min_area)
            
        if max_area:
            query += " AND area <= ?"
            params.append(max_area)
            
        if residential_complex:
            query += " AND residential_complex = ?"
            params.append(residential_complex)
            
        if is_rental is not None:
            if is_rental:
                query += " AND table_type = 'rental'"
            else:
                query += " AND table_type = 'sale'"
        
        query += " ORDER BY price ASC LIMIT ?"
        params.append(limit)
        
        cursor = db.conn.execute(query, params)
        results = [dict(row) for row in cursor.fetchall()]
        
        db.disconnect()
        
        return {
            "success": True,
            "count": len(results),
            "flats": results
        }
        
    except Exception as e:
        logger.error(f"Error searching flats: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/{flat_id}")
async def get_flat_details(flat_id: str):
    """
    Get detailed information about a specific flat.
    """
    try:
        db = OrthancDB("flats.db")
        db.connect()
        
        # Try rental_flats first
        cursor = db.conn.execute("SELECT * FROM rental_flats WHERE flat_id = ?", (flat_id,))
        rental_flat = cursor.fetchone()
        
        if rental_flat:
            db.disconnect()
            return {
                "success": True,
                "flat": dict(rental_flat),
                "type": "rental"
            }
        
        # Try sales_flats
        cursor = db.conn.execute("SELECT * FROM sales_flats WHERE flat_id = ?", (flat_id,))
        sales_flat = cursor.fetchone()
        
        if sales_flat:
            db.disconnect()
            return {
                "success": True,
                "flat": dict(sales_flat),
                "type": "sale"
            }
        
        db.disconnect()
        raise HTTPException(status_code=404, detail=f"Flat {flat_id} not found")
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting flat details: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/stats/summary")
async def get_flats_summary():
    """
    Get summary statistics for all flats.
    """
    try:
        db = OrthancDB("flats.db")
        db.connect()
        
        # Get rental stats
        cursor = db.conn.execute("""
            SELECT 
                COUNT(*) as count,
                AVG(price) as avg_price,
                MIN(price) as min_price,
                MAX(price) as max_price,
                AVG(area) as avg_area
            FROM rental_flats
        """)
        rental_stats = dict(cursor.fetchone())
        
        # Get sales stats
        cursor = db.conn.execute("""
            SELECT 
                COUNT(*) as count,
                AVG(price) as avg_price,
                MIN(price) as min_price,
                MAX(price) as max_price,
                AVG(area) as avg_area
            FROM sales_flats
        """)
        sales_stats = dict(cursor.fetchone())
        
        db.disconnect()
        
        return {
            "success": True,
            "rentals": rental_stats,
            "sales": sales_stats
        }
        
    except Exception as e:
        logger.error(f"Error getting flats summary: {e}")
        raise HTTPException(status_code=500, detail=str(e))
