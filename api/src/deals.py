"""
Deal sheet API endpoints.

GET /api/deals/sheet-url/{flat_id} — returns the spreadsheet URL for a flat
that already has a column in the deals sheet.
"""

import logging

from fastapi import APIRouter, HTTPException

from api.src.deals_sheet import DealsSheetClient

logger = logging.getLogger(__name__)
router = APIRouter()


@router.get("/sheet-url/{flat_id}")
async def get_sheet_url(flat_id: str):
    """Return the deals spreadsheet URL for a flat that has been written."""
    client = DealsSheetClient()
    url = client.get_sheet_url_for_flat(flat_id)
    if url is None:
        raise HTTPException(
            status_code=404,
            detail=f"No deals column found for flat {flat_id}",
        )
    return {"url": url, "flat_id": flat_id}
