"""Portfolio API endpoint."""

from fastapi import APIRouter

from api.src.deals_sheet import DealsSheetClient

router = APIRouter()


@router.get("")
async def get_portfolio():
    """Return active and completed deals from the deals spreadsheet."""
    return DealsSheetClient().read_portfolio()
