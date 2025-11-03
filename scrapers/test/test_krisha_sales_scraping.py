"""
Test JK sales scraping functionality with strict assertions and FlatType enum.

python -m pytest scrapers/test/test_krisha_sales_scraping.py -v -s --log-cli-level=INFO
"""

import logging

from scrapers.src.krisha_sales_scraping import (
    scrape_sales_flat_from_analytics_page,
    scrape_sales_flat_from_analytics_page_with_failover_to_sale_page,
    scrape_sales_flat_from_sale_page,
)
from common.src.flat_info import FlatInfo
from common.src.flat_type import FlatType

# Known sales listings
# https://krisha.kz/a/show/1006411384
TEST_SALES_KRISHA_ID_1 = "1006411384"
TEST_SALES_KRISHA_ID_2 = "1003480833"

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class TestSalesScraping:
    """Test class for sales flat scraping functionality."""

    def test_scrape_sales_flat_success_1(self):
        logger.info(f"Testing sales scraping for Krisha ID: {TEST_SALES_KRISHA_ID_1}")
        logger.info(f"URL: https://krisha.kz/a/show/{TEST_SALES_KRISHA_ID_1}")

        flat_info = scrape_sales_flat_from_analytics_page(TEST_SALES_KRISHA_ID_1)

        # Fail if None
        assert flat_info is not None, "Failed to scrape sales flat - returned None."

        # Log fields
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

        # Strict known facts
        assert flat_info.flat_id == TEST_SALES_KRISHA_ID_1
        assert flat_info.price == 22_000_000
        assert flat_info.area == 31
        assert flat_info.floor == 5
        assert flat_info.total_floors == 5
        assert flat_info.is_rental is False
        assert flat_info.flat_type == FlatType.ONE_BEDROOM.value
        assert flat_info.residential_complex is None

    def test_scrape_sales_flat_success_2(self):
        logger.info(f"Testing sales scraping for Krisha ID: {TEST_SALES_KRISHA_ID_2}")
        logger.info(f"URL: https://krisha.kz/a/show/{TEST_SALES_KRISHA_ID_2}")

        flat_info = scrape_sales_flat_from_analytics_page(TEST_SALES_KRISHA_ID_2)

        # Fail if None
        assert flat_info is not None, "Failed to scrape sales flat - returned None."

        # Log fields
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

        # Strict known facts
        assert flat_info.flat_id == TEST_SALES_KRISHA_ID_2
        assert flat_info.price == 135_000_000
        assert flat_info.area == 112
        assert flat_info.floor == 2
        assert flat_info.total_floors == 9
        assert flat_info.is_rental is False
        assert flat_info.flat_type == FlatType.THREE_PLUS_BEDROOM.value
        assert flat_info.residential_complex == "Meridian Apartments"

    def test_flat_info_structure(self):
        """Test the structure and type of the returned FlatInfo object."""
        logger.info("Testing FlatInfo object structure...")

        flat_info = scrape_sales_flat_from_analytics_page(TEST_SALES_KRISHA_ID_1)

        # CRITICAL: Test should fail if scraping returns None
        assert flat_info is not None, (
            "Cannot test structure - flat_info is None. This indicates the scraper is not working properly."
        )

        # Verify it's a FlatInfo instance
        assert isinstance(flat_info, FlatInfo), (
            f"Expected FlatInfo instance, got {type(flat_info)}"
        )

        # Verify field types
        assert isinstance(flat_info.flat_id, str), (
            f"flat_id should be str, got {type(flat_info.flat_id)}"
        )
        assert isinstance(flat_info.price, (int, float)), (
            f"price should be int/float, got {type(flat_info.price)}"
        )
        assert isinstance(flat_info.area, (int, float)), (
            f"area should be int/float, got {type(flat_info.area)}"
        )
        assert isinstance(flat_info.is_rental, bool), (
            f"is_rental should be bool, got {type(flat_info.is_rental)}"
        )

        logger.info("✅ FlatInfo structure validation passed!")

    def test_scrape_sales_flat_with_failover_success_1(self):
        """Test the failover function with the first test ID."""
        logger.info(
            f"Testing sales scraping with failover for Krisha ID: {TEST_SALES_KRISHA_ID_1}"
        )
        logger.info(f"URL: https://krisha.kz/a/show/{TEST_SALES_KRISHA_ID_1}")

        flat_info = scrape_sales_flat_from_analytics_page_with_failover_to_sale_page(
            TEST_SALES_KRISHA_ID_1
        )

        # Fail if None
        assert flat_info is not None, (
            "Failed to scrape sales flat with failover - returned None."
        )

        # Log fields
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

        # Strict known facts
        assert flat_info.flat_id == TEST_SALES_KRISHA_ID_1
        assert flat_info.price == 22_000_000
        assert flat_info.area == 31
        assert flat_info.floor == 5
        assert flat_info.total_floors == 5
        assert flat_info.is_rental is False
        assert flat_info.flat_type == FlatType.ONE_BEDROOM.value
        assert flat_info.residential_complex is None

    def test_scrape_sales_flat_with_failover_success_2(self):
        """Test the failover function with the second test ID."""
        logger.info(
            f"Testing sales scraping with failover for Krisha ID: {TEST_SALES_KRISHA_ID_2}"
        )
        logger.info(f"URL: https://krisha.kz/a/show/{TEST_SALES_KRISHA_ID_2}")

        flat_info = scrape_sales_flat_from_analytics_page_with_failover_to_sale_page(
            TEST_SALES_KRISHA_ID_2
        )

        # Fail if None
        assert flat_info is not None, (
            "Failed to scrape sales flat with failover - returned None."
        )

        # Log fields
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

        # Strict known facts
        assert flat_info.flat_id == TEST_SALES_KRISHA_ID_2
        assert flat_info.price == 135_000_000
        assert flat_info.area == 112
        assert flat_info.floor == 2
        assert flat_info.total_floors == 9
        assert flat_info.is_rental is False
        assert flat_info.flat_type == FlatType.THREE_PLUS_BEDROOM.value
        assert flat_info.residential_complex == "Meridian Apartments"

    def test_scrape_sales_flat_from_sales_page_success_1(self):
        """Test direct sales page scraping with the first test ID."""
        logger.info(
            f"Testing direct sales page scraping for Krisha ID: {TEST_SALES_KRISHA_ID_1}"
        )
        logger.info(f"URL: https://krisha.kz/a/show/{TEST_SALES_KRISHA_ID_1}")

        flat_info = scrape_sales_flat_from_sale_page(TEST_SALES_KRISHA_ID_1)

        # Fail if None
        assert flat_info is not None, (
            "Failed to scrape sales flat from sales page - returned None."
        )

        # Log fields
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

        # Strict known facts (same as analytics API test)
        assert flat_info.flat_id == TEST_SALES_KRISHA_ID_1
        assert flat_info.price == 22_000_000
        assert flat_info.area == 31
        assert flat_info.floor == 5
        assert flat_info.total_floors == 5
        assert flat_info.is_rental is False
        assert flat_info.flat_type == FlatType.ONE_BEDROOM.value
        assert flat_info.residential_complex is None

    def test_scrape_sales_flat_from_sales_page_success_2(self):
        """Test direct sales page scraping with the second test ID."""
        logger.info(
            f"Testing direct sales page scraping for Krisha ID: {TEST_SALES_KRISHA_ID_2}"
        )
        logger.info(f"URL: https://krisha.kz/a/show/{TEST_SALES_KRISHA_ID_2}")

        flat_info = scrape_sales_flat_from_sale_page(TEST_SALES_KRISHA_ID_2)

        # Fail if None
        assert flat_info is not None, (
            "Failed to scrape sales flat from sales page - returned None."
        )

        # Log fields
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

        # Strict known facts (same as analytics API test)
        assert flat_info.flat_id == TEST_SALES_KRISHA_ID_2
        assert flat_info.price == 135_000_000
        assert flat_info.area == 112
        assert flat_info.floor == 2
        assert flat_info.total_floors == 9
        assert flat_info.is_rental is False
        assert flat_info.flat_type == FlatType.THREE_PLUS_BEDROOM.value
        assert flat_info.residential_complex == "Meridian Apartments"

    def test_scrape_sale_flat(self):
        """
        Test successful rental flat scraping.

        This test verifies that the scraper can successfully extract
        all required fields from a rental flat listing.
        """
        krisha_id = 1005617823
        logger.info(f"Testing rental scraping for Krisha ID: {krisha_id}")
        logger.info(f"URL: https://krisha.kz/a/show/{krisha_id}")

        # Scrape the rental flat
        flat_info = scrape_sales_flat_from_sale_page(krisha_id)

        # CRITICAL: Test should fail if scraping returns None
        assert flat_info is not None
        logger.info("Successfully scraped rental flat!")
        logger.info("=" * 50)
        logger.info("FLAT INFO FIELDS:")
        logger.info("=" * 50)

        # Check and log all FlatInfo fields
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
        logger.info("=" * 50)
