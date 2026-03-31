"""Portfolio API endpoint."""

import logging

from fastapi import APIRouter, HTTPException

from api.src.deals_sheet import DealsSheetClient

logger = logging.getLogger(__name__)
router = APIRouter()


@router.get("")
async def get_portfolio():
    """Return active and completed deals from the deals spreadsheet."""
    try:
        client = DealsSheetClient()
        return client.read_portfolio()
    except Exception as e:
        logger.exception("Failed to read deals from spreadsheet")
        raise HTTPException(status_code=502, detail=f"Sheet read failed: {e}")
