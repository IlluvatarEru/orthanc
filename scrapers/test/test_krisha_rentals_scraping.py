"""
Test rental scraping functionality.

These tests hit live Krisha.kz endpoints. Tests that depend on specific flat IDs
will skip gracefully if the flat is no longer available (archived/removed).

python -m pytest scrapers/test/test_krisha_rentals_scraping.py -v -s --log-cli-level=INFO
"""

import logging

import pytest

from scrapers.src.krisha_rental_scraping import (
    scrape_rental_flat,
    scrape_rental_flat_from_analytics_page_with_failover_to_rental_page,
    scrape_rental_flat_from_rental_page,
)
from common.src.flat_info import FlatInfo

# Known rental listings (may become archived over time)
# https://krisha.kz/a/show/1006270605
TEST_RENTAL_KRISHA_ID_1 = "1006270605"
TEST_RENTAL_KRISHA_ID_2 = "1006381629"

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


class TestRentalsScraping:
    """Test class for rental flat scraping functionality."""

    def test_scrape_rental_flat_analytics_1(self):
        """Test analytics API scraping for rental flat."""
        logger.info(f"Testing rental scraping for Krisha ID: {TEST_RENTAL_KRISHA_ID_1}")

        flat_info = scrape_rental_flat(TEST_RENTAL_KRISHA_ID_1)
        if flat_info is None:
            pytest.skip(
                f"Flat {TEST_RENTAL_KRISHA_ID_1} no longer available on Krisha analytics API"
            )

        _log_flat_info(flat_info)

        assert flat_info.flat_id == TEST_RENTAL_KRISHA_ID_1
        assert flat_info.price > 0
        assert flat_info.area > 0
        assert flat_info.is_rental is True

    def test_scrape_rental_flat_analytics_2(self):
        """Test analytics API scraping for second rental flat."""
        logger.info(f"Testing rental scraping for Krisha ID: {TEST_RENTAL_KRISHA_ID_2}")

        flat_info = scrape_rental_flat(TEST_RENTAL_KRISHA_ID_2)
        if flat_info is None:
            pytest.skip(
                f"Flat {TEST_RENTAL_KRISHA_ID_2} no longer available on Krisha analytics API"
            )

        _log_flat_info(flat_info)

        assert flat_info.flat_id == TEST_RENTAL_KRISHA_ID_2
        assert flat_info.price > 0
        assert flat_info.area > 0
        assert flat_info.is_rental is True

    def test_flat_info_structure(self):
        """Test the structure and type of the returned FlatInfo object."""
        logger.info("Testing FlatInfo object structure...")

        flat_info = scrape_rental_flat_from_analytics_page_with_failover_to_rental_page(
            TEST_RENTAL_KRISHA_ID_1
        )
        if flat_info is None:
            pytest.skip(f"Flat {TEST_RENTAL_KRISHA_ID_1} no longer available on Krisha")

        assert isinstance(flat_info, FlatInfo)
        assert isinstance(flat_info.flat_id, str)
        assert isinstance(flat_info.price, (int, float))
        assert isinstance(flat_info.area, (int, float))
        assert isinstance(flat_info.is_rental, bool)

    def test_scrape_rental_flat(self):
        """Test rental scraping with failover for a known flat."""
        krisha_id = 1006362898
        logger.info(f"Testing rental scraping for Krisha ID: {krisha_id}")

        flat_info = scrape_rental_flat_from_analytics_page_with_failover_to_rental_page(
            krisha_id
        )
        if flat_info is None:
            pytest.skip(f"Flat {krisha_id} no longer available on Krisha")

        _log_flat_info(flat_info)

        assert flat_info.price > 0
        assert flat_info.area > 0

    def test_scrape_rental_flat_with_failover_1(self):
        """Test the failover function with the first test ID."""
        logger.info(f"Testing failover scraping for: {TEST_RENTAL_KRISHA_ID_1}")

        flat_info = scrape_rental_flat_from_analytics_page_with_failover_to_rental_page(
            TEST_RENTAL_KRISHA_ID_1
        )
        if flat_info is None:
            pytest.skip(f"Flat {TEST_RENTAL_KRISHA_ID_1} no longer available on Krisha")

        _log_flat_info(flat_info)

        assert flat_info.flat_id == TEST_RENTAL_KRISHA_ID_1
        assert flat_info.price > 0
        assert flat_info.area > 0
        assert flat_info.is_rental is True

    def test_scrape_rental_flat_with_failover_2(self):
        """Test the failover function with the second test ID."""
        logger.info(f"Testing failover scraping for: {TEST_RENTAL_KRISHA_ID_2}")

        flat_info = scrape_rental_flat_from_analytics_page_with_failover_to_rental_page(
            TEST_RENTAL_KRISHA_ID_2
        )
        if flat_info is None:
            pytest.skip(f"Flat {TEST_RENTAL_KRISHA_ID_2} no longer available on Krisha")

        _log_flat_info(flat_info)

        assert flat_info.flat_id == TEST_RENTAL_KRISHA_ID_2
        assert flat_info.price > 0
        assert flat_info.area > 0
        assert flat_info.is_rental is True

    def test_scrape_rental_flat_from_rental_page_1(self):
        """Test direct rental page scraping with the first test ID."""
        logger.info(f"Testing direct page scraping for: {TEST_RENTAL_KRISHA_ID_1}")

        flat_info = scrape_rental_flat_from_rental_page(TEST_RENTAL_KRISHA_ID_1)
        if flat_info is None:
            pytest.skip(f"Flat {TEST_RENTAL_KRISHA_ID_1} no longer available on Krisha")

        _log_flat_info(flat_info)

        assert flat_info.flat_id == TEST_RENTAL_KRISHA_ID_1
        assert flat_info.price > 0
        assert flat_info.area > 0
        assert flat_info.is_rental is True

    def test_scrape_rental_flat_from_rental_page_2(self):
        """Test direct rental page scraping with the second test ID."""
        logger.info(f"Testing direct page scraping for: {TEST_RENTAL_KRISHA_ID_2}")

        flat_info = scrape_rental_flat_from_rental_page(TEST_RENTAL_KRISHA_ID_2)
        if flat_info is None:
            pytest.skip(f"Flat {TEST_RENTAL_KRISHA_ID_2} no longer available on Krisha")

        _log_flat_info(flat_info)

        assert flat_info.flat_id == TEST_RENTAL_KRISHA_ID_2
        assert flat_info.price > 0
        assert flat_info.area > 0
        assert flat_info.is_rental is True
