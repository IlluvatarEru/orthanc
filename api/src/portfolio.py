"""Portfolio API endpoint."""

import logging

from fastapi import APIRouter, HTTPException

from api.src.deals_sheet import DealsSheetClient

logger = logging.getLogger(__name__)
router = APIRouter()


@router.get("")
async def get_portfolio():
    """Return completed deals from the deals spreadsheet with summary stats."""
    try:
        client = DealsSheetClient()
        return client.read_completed_deals()
    except Exception as e:
        logger.exception("Failed to read deals from spreadsheet")
        raise HTTPException(status_code=502, detail=f"Sheet read failed: {e}")
