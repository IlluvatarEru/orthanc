"""
Portfolio API endpoint.

GET /api/portfolio — returns completed deals read from the deals spreadsheet.
"""

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
        all_deals = client.read_all_deals()
    except Exception as e:
        logger.exception("Failed to read deals from spreadsheet")
        raise HTTPException(status_code=502, detail=f"Sheet read failed: {e}")

    completed = [d for d in all_deals if d["completed"]]

    total_invested = sum(d["total_cost"] or 0 for d in completed)
    total_profit_kzt = sum(d["net_profit_kzt"] or 0 for d in completed)
    total_profit_eur = sum(d["net_profit_eur"] or 0 for d in completed)

    summary = {
        "completed_count": len(completed),
        "total_invested_kzt": total_invested,
        "total_profit_kzt": total_profit_kzt,
        "total_profit_eur": total_profit_eur,
    }

    return {
        "completed": completed,
        "summary": summary,
    }
