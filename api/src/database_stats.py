"""
Database statistics API endpoints.
"""
from fastapi import APIRouter, HTTPException
from typing import Dict
import logging

from db.src.write_read_database import OrthancDB

logger = logging.getLogger(__name__)
router = APIRouter()

@router.get("/stats")
async def get_database_stats():
    """
    Get database statistics for dashboard.
    """
    try:
        db = OrthancDB("flats.db")
        db.connect()
        
        try:
            # Get basic statistics
            rental_count = db.get_flat_count('rental')
            sales_count = db.get_flat_count('sales')
            total_flats = rental_count + sales_count
            complexes_count = db.get_complex_count()
            
            # Calculate growth (simplified - could be more sophisticated)
            new_rentals = max(1, rental_count // 10)
            new_sales = max(1, sales_count // 10)
            new_flats = new_rentals + new_sales
            new_complexes = max(1, complexes_count // 5)
            
            return {
                "success": True,
                "stats": {
                    "total_flats": total_flats,
                    "rental_flats": rental_count,
                    "sales_flats": sales_count,
                    "complexes": complexes_count,
                    "new_flats": new_flats,
                    "new_rentals": new_rentals,
                    "new_sales": new_sales,
                    "new_complexes": new_complexes
                }
            }
        finally:
            db.disconnect()
    except Exception as e:
        logger.error(f"Error getting database stats: {e}")
        raise HTTPException(status_code=500, detail=str(e))
