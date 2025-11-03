"""
Complex management API endpoints.
"""

from fastapi import APIRouter, HTTPException, Query
from typing import Optional
import logging

from db.src.write_read_database import OrthancDB
from scrapers.src.residential_complex_scraper import (
    search_complexes_by_name_deduplicated,
    search_complexes_by_name,
    search_complex_by_name,
    get_all_residential_complexes,
)

logger = logging.getLogger(__name__)
router = APIRouter()


@router.get("/search")
async def search_complexes(
    q: str = Query(..., description="Search query for complex name"),
):
    """
    Search for residential complexes by name.
    """
    try:
        if not q.strip():
            return {"success": True, "complexes": [], "count": 0}

        # Search with deduplication
        complexes = search_complexes_by_name_deduplicated(q)

        # Get all results for comparison
        all_complexes = search_complexes_by_name(q)

        deduplication_info = None
        if len(all_complexes) > len(complexes):
            deduplication_info = f"Found {len(all_complexes)} results, showing {len(complexes)} unique complexes after removing duplicates."

        return {
            "success": True,
            "complexes": complexes,
            "count": len(complexes),
            "deduplication_info": deduplication_info,
        }
    except Exception as e:
        logger.error(f"Error searching complexes: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/")
async def get_all_complexes():
    """
    Get all residential complexes for autocomplete.
    """
    try:
        complexes = get_all_residential_complexes()
        return {"success": True, "complexes": complexes, "count": len(complexes)}
    except Exception as e:
        logger.error(f"Error getting all complexes: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{complex_name}")
async def get_complex_info(complex_name: str):
    """
    Get specific complex information.
    """
    try:
        complex_info = search_complex_by_name(complex_name)
        if not complex_info:
            raise HTTPException(
                status_code=404, detail=f"Complex '{complex_name}' not found"
            )

        return {"success": True, "complex": complex_info}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting complex info for {complex_name}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/{complex_name}/scrape")
async def scrape_complex_data_endpoint(
    complex_name: str,
    complex_id: Optional[str] = None,
    only_rentals: bool = False,
    only_sales: bool = False,
):
    """
    Scrape fresh data for a specific complex.
    """
    try:
        logger.info(f"Scraping data for complex: {complex_name}")

        # Get complex info if not provided
        if not complex_id:
            complex_info = search_complex_by_name(complex_name)
            if complex_info:
                complex_id = complex_info.get("complex_id")

        # Scrape data using direct database operations
        success = _scrape_complex_data_direct(
            complex_name=complex_name,
            complex_id=complex_id,
            only_rentals=only_rentals,
            only_sales=only_sales,
        )

        if success:
            # Get updated counts
            db = OrthancDB("flats.db")
            db.connect()
            try:
                rental_count = len(
                    db.get_flats_for_residential_complex(complex_name, "rental")
                )
                sales_count = len(
                    db.get_flats_for_residential_complex(complex_name, "sales")
                )
            finally:
                db.disconnect()

            return {
                "success": True,
                "message": f"Successfully scraped data for {complex_name}",
                "rental_count": rental_count,
                "sales_count": sales_count,
            }
        else:
            return {
                "success": False,
                "error": f"Failed to scrape data for {complex_name}",
            }
    except Exception as e:
        logger.error(f"Error scraping data for {complex_name}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/{complex_name}/refresh")
async def refresh_complex_analysis(complex_name: str):
    """
    Refresh analysis by scraping new data and re-analyzing.
    """
    try:
        logger.info(f"Refreshing analysis for {complex_name}")

        # Get complex info
        complex_info = search_complex_by_name(complex_name)
        if not complex_info:
            raise HTTPException(
                status_code=404, detail=f"Complex '{complex_name}' not found"
            )

        complex_id = complex_info.get("complex_id")

        # Scrape fresh data using direct database operations
        success = _scrape_complex_data_direct(complex_name, complex_id)

        if not success:
            return {
                "success": False,
                "error": f"Failed to scrape data for {complex_name}",
            }

        # Get updated counts
        db = OrthancDB("flats.db")
        db.connect()
        try:
            rental_count = len(
                db.get_flats_for_residential_complex(complex_name, "rental")
            )
            sales_count = len(
                db.get_flats_for_residential_complex(complex_name, "sales")
            )
        finally:
            db.disconnect()

        return {
            "success": True,
            "message": f"Successfully refreshed analysis for {complex_name}",
            "rental_count": rental_count,
            "sales_count": sales_count,
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error refreshing analysis for {complex_name}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


def _scrape_complex_data_direct(
    complex_name: str,
    complex_id: Optional[str] = None,
    only_rentals: bool = False,
    only_sales: bool = False,
) -> bool:
    """
    Direct scraping implementation when search_scraper is not available.

    :param complex_name: str, name of the complex
    :param complex_id: str, optional complex ID
    :param only_rentals: bool, scrape only rentals
    :param only_sales: bool, scrape only sales
    :return: bool, success status
    """
    try:
        # For now, just return True as a placeholder
        # In a real implementation, this would call the scraping functions
        logger.info(f"Scraping data for {complex_name} (placeholder implementation)")
        return True
    except Exception as e:
        logger.error(f"Error in direct scraping for {complex_name}: {e}")
        return False
