"""
JK (Residential Complex) rentals analysis endpoints.
"""
from fastapi import APIRouter, HTTPException, Query
from typing import List, Optional
import logging
import sys
import os

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from db.src.write_read_database import OrthancDB

logger = logging.getLogger(__name__)
router = APIRouter()

@router.get("/")
async def get_jk_rentals_list():
    """
    Get list of residential complexes with rental data.
    """
    try:
        db = OrthancDB("flats.db")
        db.connect()
        
        cursor = db.conn.execute("""
            SELECT DISTINCT residential_complex, COUNT(*) as rental_count
            FROM rental_flats 
            WHERE residential_complex IS NOT NULL
            GROUP BY residential_complex
            ORDER BY rental_count DESC
        """)
        
        jks = [{"name": row[0], "rental_count": row[1]} for row in cursor.fetchall()]
        
        db.disconnect()
        
        return {
            "success": True,
            "jks": jks,
            "count": len(jks)
        }
    except Exception as e:
        logger.error(f"Error getting JK rentals list: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/{jk_name}/summary")
async def get_jk_rentals_summary(jk_name: str):
    """
    Get rental summary for a specific residential complex.
    """
    try:
        db = OrthancDB("flats.db")
        db.connect()
        
        # Get rental statistics
        cursor = db.conn.execute("""
            SELECT 
                COUNT(*) as count,
                AVG(price) as avg_price,
                MIN(price) as min_price,
                MAX(price) as max_price,
                AVG(area) as avg_area,
                MIN(area) as min_area,
                MAX(area) as max_area
            FROM rental_flats 
            WHERE residential_complex = ?
        """, (jk_name,))
        
        stats = dict(cursor.fetchone())
        
        # Get flat type breakdown
        cursor = db.conn.execute("""
            SELECT 
                flat_type,
                COUNT(*) as count,
                AVG(price) as avg_price,
                MIN(price) as min_price,
                MAX(price) as max_price
            FROM rental_flats 
            WHERE residential_complex = ?
            GROUP BY flat_type
            ORDER BY count DESC
        """, (jk_name,))
        
        flat_type_breakdown = [dict(row) for row in cursor.fetchall()]
        
        db.disconnect()
        
        return {
            "success": True,
            "jk_name": jk_name,
            "summary": {
                "overall_stats": stats,
                "flat_type_breakdown": flat_type_breakdown
            }
        }
    except Exception as e:
        logger.error(f"Error getting rentals summary for JK {jk_name}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/{jk_name}/rentals")
async def get_jk_rentals(
    jk_name: str,
    flat_type: Optional[str] = Query(None, description="Filter by flat type"),
    min_price: Optional[int] = Query(None, description="Minimum price filter"),
    max_price: Optional[int] = Query(None, description="Maximum price filter"),
    limit: int = Query(50, description="Maximum number of results")
):
    """
    Get rental listings for a specific residential complex.
    """
    try:
        db = OrthancDB("flats.db")
        db.connect()
        
        query = "SELECT * FROM rental_flats WHERE residential_complex = ?"
        params = [jk_name]
        
        if flat_type:
            query += " AND flat_type = ?"
            params.append(flat_type)
            
        if min_price:
            query += " AND price >= ?"
            params.append(min_price)
            
        if max_price:
            query += " AND price <= ?"
            params.append(max_price)
        
        query += " ORDER BY price ASC LIMIT ?"
        params.append(limit)
        
        cursor = db.conn.execute(query, params)
        rentals = [dict(row) for row in cursor.fetchall()]
        
        db.disconnect()
        
        return {
            "success": True,
            "jk_name": jk_name,
            "rentals": rentals,
            "count": len(rentals)
        }
    except Exception as e:
        logger.error(f"Error getting rentals for JK {jk_name}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/{jk_name}/price-trends")
async def get_jk_rental_price_trends(jk_name: str):
    """
    Get rental price trends for a residential complex over time.
    """
    try:
        db = OrthancDB("flats.db")
        db.connect()
        
        # Get price trends by date
        cursor = db.conn.execute("""
            SELECT 
                DATE(query_date) as date,
                flat_type,
                COUNT(*) as count,
                AVG(price) as avg_price,
                MIN(price) as min_price,
                MAX(price) as max_price
            FROM rental_flats 
            WHERE residential_complex = ?
            GROUP BY DATE(query_date), flat_type
            ORDER BY date DESC, flat_type
        """, (jk_name,))
        
        trends = [dict(row) for row in cursor.fetchall()]
        
        db.disconnect()
        
        return {
            "success": True,
            "jk_name": jk_name,
            "price_trends": trends
        }
    except Exception as e:
        logger.error(f"Error getting rental price trends for JK {jk_name}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/stats/overview")
async def get_rentals_overview():
    """
    Get overall rental market overview.
    """
    try:
        db = OrthancDB("flats.db")
        db.connect()
        
        # Overall rental stats
        cursor = db.conn.execute("""
            SELECT 
                COUNT(*) as total_rentals,
                COUNT(DISTINCT residential_complex) as total_jks,
                AVG(price) as avg_price,
                MIN(price) as min_price,
                MAX(price) as max_price,
                AVG(area) as avg_area
            FROM rental_flats
        """)
        
        overall_stats = dict(cursor.fetchone())
        
        # Top JKs by rental count
        cursor = db.conn.execute("""
            SELECT 
                residential_complex,
                COUNT(*) as rental_count,
                AVG(price) as avg_price
            FROM rental_flats 
            WHERE residential_complex IS NOT NULL
            GROUP BY residential_complex
            ORDER BY rental_count DESC
            LIMIT 10
        """)
        
        top_jks = [dict(row) for row in cursor.fetchall()]
        
        # Flat type distribution
        cursor = db.conn.execute("""
            SELECT 
                flat_type,
                COUNT(*) as count,
                AVG(price) as avg_price
            FROM rental_flats
            GROUP BY flat_type
            ORDER BY count DESC
        """)
        
        flat_type_distribution = [dict(row) for row in cursor.fetchall()]
        
        db.disconnect()
        
        return {
            "success": True,
            "overview": {
                "overall_stats": overall_stats,
                "top_jks": top_jks,
                "flat_type_distribution": flat_type_distribution
            }
        }
    except Exception as e:
        logger.error(f"Error getting rentals overview: {e}")
        raise HTTPException(status_code=500, detail=str(e))
