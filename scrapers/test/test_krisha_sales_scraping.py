"""
Test JK sales scraping functionality with strict assertions and FlatType enum.

These tests hit live Krisha.kz endpoints. Tests that depend on specific flat IDs
will skip gracefully if the flat is no longer available (archived/removed).

python -m pytest scrapers/test/test_krisha_sales_scraping.py -v -s --log-cli-level=INFO
"""

import logging

import pytest

from scrapers.src.krisha_sales_scraping import (
    scrape_sales_flat_from_analytics_page,
    scrape_sales_flat_from_analytics_page_with_failover_to_sale_page,
    scrape_sales_flat_from_sale_page,
)
from common.src.flat_info import FlatInfo
from common.src.flat_type import FlatType

# Known sales listings (may become archived over time)
# https://krisha.kz/a/show/1003480833
TEST_SALES_KRISHA_ID_1 = "1003480833"

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def _log_flat_info(flat_info):
    """Helper to log all FlatInfo fields."""
    fields_to_check = [
        ("flat_id", flat_info.flat_id),
        ("price", flat_info.price),
        ("area", flat_info.area),
        ("flat_type", flat_info.flat_type),
        ("residential_complex", flat_info.residential_complex),
        ("floor", flat_info.floor),
        ("total_floors", flat_info.total_floors),
        ("construction_year", flat_info.construction_year),
        ("parking", flat_info.parking),
        ("description", flat_info.description),
        ("is_rental", flat_info.is_rental),
    ]
    for field_name, field_value in fields_to_check:
        logger.info(f"{field_name:20}: {field_value}")


class TestSalesScraping:
    """Test class for sales flat scraping functionality."""

    def test_scrape_sales_flat_from_analytics(self):
        """Test analytics page scraping."""
        logger.info(f"Testing sales scraping for Krisha ID: {TEST_SALES_KRISHA_ID_1}")

        flat_info = scrape_sales_flat_from_analytics_page(TEST_SALES_KRISHA_ID_1)
        if flat_info is None:
            pytest.skip(
                f"Flat {TEST_SALES_KRISHA_ID_1} no longer available on Krisha analytics API"
            )

        _log_flat_info(flat_info)

        # Stable facts (don't assert exact price - it can change)
        assert flat_info.flat_id == TEST_SALES_KRISHA_ID_1
        assert flat_info.area == 112
        assert flat_info.floor == 2
        assert flat_info.total_floors == 9
        assert flat_info.is_rental is False
        assert flat_info.flat_type == FlatType.THREE_PLUS_BEDROOM.value
        assert flat_info.residential_complex == "Meridian Apartments"
        assert flat_info.price > 0

    def test_flat_info_structure(self):
        """Test the structure and type of the returned FlatInfo object."""
        logger.info("Testing FlatInfo object structure...")

        flat_info = scrape_sales_flat_from_analytics_page(TEST_SALES_KRISHA_ID_1)
        if flat_info is None:
            pytest.skip(
                f"Flat {TEST_SALES_KRISHA_ID_1} no longer available on Krisha analytics API"
            )

        assert isinstance(flat_info, FlatInfo)
        assert isinstance(flat_info.flat_id, str)
        assert isinstance(flat_info.price, (int, float))
        assert isinstance(flat_info.area, (int, float))
        assert isinstance(flat_info.is_rental, bool)

    def test_scrape_sales_flat_with_failover(self):
        """Test the failover function (analytics -> sale page)."""
        logger.info(
            f"Testing sales scraping with failover for: {TEST_SALES_KRISHA_ID_1}"
        )

        flat_info = scrape_sales_flat_from_analytics_page_with_failover_to_sale_page(
            TEST_SALES_KRISHA_ID_1
        )
        if flat_info is None:
            pytest.skip(f"Flat {TEST_SALES_KRISHA_ID_1} no longer available on Krisha")

        _log_flat_info(flat_info)

        assert flat_info.flat_id == TEST_SALES_KRISHA_ID_1
        assert flat_info.area == 112
        assert flat_info.floor == 2
        assert flat_info.total_floors == 9
        assert flat_info.is_rental is False
        assert flat_info.flat_type == FlatType.THREE_PLUS_BEDROOM.value
        assert flat_info.residential_complex == "Meridian Apartments"
        assert flat_info.price > 0

    def test_scrape_sales_flat_from_sale_page(self):
        """Test direct sale page scraping."""
        logger.info(f"Testing direct sale page scraping for: {TEST_SALES_KRISHA_ID_1}")

        flat_info = scrape_sales_flat_from_sale_page(TEST_SALES_KRISHA_ID_1)
        if flat_info is None:
            pytest.skip(f"Flat {TEST_SALES_KRISHA_ID_1} no longer available on Krisha")

        _log_flat_info(flat_info)

        assert flat_info.flat_id == TEST_SALES_KRISHA_ID_1
        assert flat_info.area == 112
        assert flat_info.floor == 2
        assert flat_info.total_floors == 9
        assert flat_info.is_rental is False
        assert flat_info.flat_type == FlatType.THREE_PLUS_BEDROOM.value
        assert flat_info.residential_complex == "Meridian Apartments"
        assert flat_info.price > 0

    def test_scrape_sale_flat(self):
        """Test that scraping returns correct data types and non-empty values."""
        flat_info = scrape_sales_flat_from_analytics_page_with_failover_to_sale_page(
            TEST_SALES_KRISHA_ID_1
        )
        if flat_info is None:
            pytest.skip(f"Flat {TEST_SALES_KRISHA_ID_1} no longer available on Krisha")

        # Validate data quality
        assert flat_info.price > 0, "Price should be positive"
        assert flat_info.area > 0, "Area should be positive"
        assert flat_info.flat_type in [ft.value for ft in FlatType], (
            f"Invalid flat type: {flat_info.flat_type}"
        )
        assert flat_info.description, "Description should not be empty"

    def test_city_classification_astana_ak_bota(self):
        """Test that Ак Бота flat 1009432578 in Astana is classified as Астана."""
        flat_info = scrape_sales_flat_from_analytics_page("1009432578")
        if flat_info is None:
            pytest.skip("Flat 1009432578 no longer available on Krisha")

        assert flat_info.city == "Астана", (
            f"Flat 1009432578 (Ак Бота) should be Астана, got {flat_info.city}"
        )
        assert flat_info.residential_complex == "Ак Бота"

    def test_city_classification_astana_1008682324(self):
        """Test that flat 1008682324 in Astana is classified as Астана."""
        flat_info = scrape_sales_flat_from_analytics_page("1008682324")
        if flat_info is None:
            pytest.skip("Flat 1008682324 no longer available on Krisha")

        assert flat_info.city == "Астана", (
            f"Flat 1008682324 should be Астана, got {flat_info.city}"
        )

    def test_city_classification_almaty(self):
        """Test that a known Almaty flat is classified as Алматы."""
        flat_info = scrape_sales_flat_from_analytics_page(TEST_SALES_KRISHA_ID_1)
        if flat_info is None:
            pytest.skip(f"Flat {TEST_SALES_KRISHA_ID_1} no longer available on Krisha")

        assert flat_info.city == "Алматы", (
            f"Flat {TEST_SALES_KRISHA_ID_1} (Meridian) should be Алматы, got {flat_info.city}"
        )
