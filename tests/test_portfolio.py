"""
Test portfolio endpoint — reads completed deals from the deals spreadsheet.

Tests marked @requires_sheet need the real sheet accessible via service account.

pytest tests/test_portfolio.py -v
pytest tests/test_portfolio.py -v -m "not requires_sheet"   # unit tests only
"""

import os
import pytest

from api.src.deals_sheet import DealsSheetClient

_SA_PATH = os.path.join(
    os.path.dirname(__file__), "..", "config", "google_service_account.json"
)
requires_sheet = pytest.mark.skipif(
    not os.path.exists(_SA_PATH),
    reason="Google service account JSON not found — skipping sheet tests",
)


@pytest.fixture(scope="module")
def deals():
    """Read all deals from the real spreadsheet once for the test module."""
    client = DealsSheetClient()
    return client.read_all_deals()


@pytest.fixture(scope="module")
def completed(deals):
    return [d for d in deals if d["completed"]]


class TestPortfolio:
    @requires_sheet
    def test_read_deals_returns_list(self, deals):
        assert isinstance(deals, list)
        assert len(deals) > 0, "Expected at least one deal in the spreadsheet"

    @requires_sheet
    def test_completed_deals_have_completed_true(self, completed):
        for deal in completed:
            assert deal["completed"] is True

    @requires_sheet
    def test_known_completed_deal_present(self, completed):
        # "Nekcpo" is completed per the spreadsheet
        projects = [d["project"] for d in completed]
        assert "Nekcpo" in projects

    @requires_sheet
    def test_completed_deals_have_required_fields(self, completed):
        required = [
            "project",
            "flat_id",
            "investment_date",
            "completed",
            "days_held",
        ]
        for deal in completed:
            for field in required:
                assert field in deal, (
                    f"Missing field {field} in deal {deal.get('project')}"
                )

    @requires_sheet
    def test_days_held_positive_for_completed(self, completed):
        for deal in completed:
            if deal["days_held"] is not None:
                assert deal["days_held"] >= 0, (
                    f"days_held should be >= 0 for {deal['project']}"
                )

    @requires_sheet
    def test_summary_counts_match(self, completed):
        summary_count = len(completed)
        assert summary_count > 0

    def test_parse_deal_static(self):
        """Unit test for _parse_deal without sheet access."""
        raw = {
            "Project": "TestProject",
            "Flat ID": "99999",
            "Investment Date": "25/10/2022",
            "Flat Price": "30,000,000",
            "Total Cost": "30,500,000",
            "Total Cost (EUR)": "57,547",
            "Resale Date": "15/01/2023",
            "Rent Received": "0",
            "Resale Price": "35,000,000",
            "Net Profit KZT": "4,000,000",
            "Net Profit EUR": "7,547",
            "Net Return KZT": "0.131",
            "Net Return EUR": "0.131",
            "Equivalent Annual Return EUR": "0.58",
            "Completed": "TRUE",
            "Multiple": "1.13",
            "Number of weeks": "11.7",
        }
        deal = DealsSheetClient._parse_deal(raw)
        assert deal["project"] == "TestProject"
        assert deal["flat_id"] == "99999"
        assert deal["investment_date"] == "2022-10-25"
        assert deal["completed"] is True
        assert deal["flat_price"] == 30000000.0
        assert deal["resale_date"] == "2023-01-15"
        assert deal["days_held"] is not None
        assert deal["days_held"] == 82  # 25 Oct 2022 -> 15 Jan 2023

    def test_parse_deal_not_completed(self):
        raw = {
            "Project": "ActiveDeal",
            "Flat ID": "88888",
            "Investment Date": "01/01/2025",
            "Completed": "FALSE",
        }
        deal = DealsSheetClient._parse_deal(raw)
        assert deal["completed"] is False
        assert deal["days_held"] is not None
        assert deal["days_held"] >= 0
