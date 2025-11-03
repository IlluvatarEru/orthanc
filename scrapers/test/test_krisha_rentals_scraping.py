"""
Test rental scraping functionality.

This module tests the rental flat scraping functionality using pytest.

python -m pytest scrapers/test/test_krisha_rentals_scraping.py -v -s --log-cli-level=INFO
"""

import logging

from common.src.flat_type import FlatType


from scrapers.src.krisha_rental_scraping import (
    scrape_rental_flat,
    scrape_rental_flat_from_analytics_page_with_failover_to_rental_page,
    scrape_rental_flat_from_rental_page,
)
from common.src.flat_info import FlatInfo

# Test Krisha ID for rental flat
# https://krisha.kz/a/show/1006270605
TEST_RENTAL_KRISHA_ID_1 = "1006270605"
TEST_RENTAL_KRISHA_ID_2 = "1006381629"
# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class TestRentalsScraping:
    """Test class for rental flat scraping functionality."""

    def test_scrape_rental_flat_success_1(self):
        """
        Test successful rental flat scraping.

        This test verifies that the scraper can successfully extract
        all required fields from a rental flat listing.
        """
        logger.info(f"Testing rental scraping for Krisha ID: {TEST_RENTAL_KRISHA_ID_1}")
        logger.info(f"URL: https://krisha.kz/a/show/{TEST_RENTAL_KRISHA_ID_1}")

        # Scrape the rental flat
        flat_info = scrape_rental_flat(TEST_RENTAL_KRISHA_ID_1)

        # CRITICAL: Test should fail if scraping returns None
        assert flat_info is not None, (
            "Failed to scrape rental flat - returned None. This indicates the scraper is not working properly."
        )

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

        assert flat_info.flat_id == TEST_RENTAL_KRISHA_ID_1
        assert flat_info.price == 1_100_000
        assert flat_info.area == 250
        assert flat_info.flat_type == FlatType.THREE_PLUS_BEDROOM.value
        assert flat_info.residential_complex == "Кокше"
        assert flat_info.floor == 3
        assert flat_info.total_floors == 3
        assert flat_info.construction_year is None
        assert flat_info.parking is None
        assert flat_info.is_rental

    def test_scrape_rental_flat_success_2(self):
        """
        Test successful rental flat scraping.

        This test verifies that the scraper can successfully extract
        all required fields from a rental flat listing.
        """
        logger.info(f"Testing rental scraping for Krisha ID: {TEST_RENTAL_KRISHA_ID_2}")
        logger.info(f"URL: https://krisha.kz/a/show/{TEST_RENTAL_KRISHA_ID_2}")

        # Scrape the rental flat
        flat_info = scrape_rental_flat(TEST_RENTAL_KRISHA_ID_2)

        # CRITICAL: Test should fail if scraping returns None
        assert flat_info is not None, (
            "Failed to scrape rental flat - returned None. This indicates the scraper is not working properly."
        )

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

        assert flat_info.flat_id == TEST_RENTAL_KRISHA_ID_2
        assert flat_info.price == 500_000
        assert flat_info.area == 52
        assert flat_info.flat_type == FlatType.ONE_BEDROOM.value
        assert flat_info.residential_complex == "Meridian Apartments"
        assert flat_info.floor == 2
        assert flat_info.total_floors == 12
        assert flat_info.construction_year is None
        assert flat_info.parking is None
        assert flat_info.is_rental

    def test_flat_info_structure(self):
        """Test the structure and type of the returned FlatInfo object."""
        logger.info("Testing FlatInfo object structure...")

        flat_info = scrape_rental_flat(TEST_RENTAL_KRISHA_ID_1)

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

    def test_scrape_rental_flat(self):
        """
        Test successful rental flat scraping.

        This test verifies that the scraper can successfully extract
        all required fields from a rental flat listing.
        """
        krisha_id = 1006362898
        logger.info(f"Testing rental scraping for Krisha ID: {krisha_id}")
        logger.info(f"URL: https://krisha.kz/a/show/{krisha_id}")

        # Scrape the rental flat
        flat_info = scrape_rental_flat_from_analytics_page_with_failover_to_rental_page(
            krisha_id
        )

        # CRITICAL: Test should fail if scraping returns None
        assert flat_info is not None, (
            "Failed to scrape rental flat - returned None. This indicates the scraper is not working properly."
        )

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

    def test_scrape_rental_flat_with_failover_success_1(self):
        """Test the failover function with the first test ID."""
        logger.info(
            f"Testing rental scraping with failover for Krisha ID: {TEST_RENTAL_KRISHA_ID_1}"
        )
        logger.info(f"URL: https://krisha.kz/a/show/{TEST_RENTAL_KRISHA_ID_1}")

        flat_info = scrape_rental_flat_from_analytics_page_with_failover_to_rental_page(
            TEST_RENTAL_KRISHA_ID_1
        )

        # Fail if None
        assert flat_info is not None, (
            "Failed to scrape rental flat with failover - returned None."
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
        assert flat_info.flat_id == TEST_RENTAL_KRISHA_ID_1
        assert flat_info.price == 1_100_000
        assert flat_info.area == 250
        assert flat_info.flat_type == FlatType.THREE_PLUS_BEDROOM.value
        assert flat_info.residential_complex == "Кокше"
        assert flat_info.floor == 3
        assert flat_info.total_floors == 3
        assert flat_info.construction_year is None
        assert flat_info.parking is None
        assert flat_info.is_rental

    def test_scrape_rental_flat_with_failover_success_2(self):
        """Test the failover function with the second test ID."""
        logger.info(
            f"Testing rental scraping with failover for Krisha ID: {TEST_RENTAL_KRISHA_ID_2}"
        )
        logger.info(f"URL: https://krisha.kz/a/show/{TEST_RENTAL_KRISHA_ID_2}")

        flat_info = scrape_rental_flat_from_analytics_page_with_failover_to_rental_page(
            TEST_RENTAL_KRISHA_ID_2
        )

        # Fail if None
        assert flat_info is not None, (
            "Failed to scrape rental flat with failover - returned None."
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
        assert flat_info.flat_id == TEST_RENTAL_KRISHA_ID_2
        assert flat_info.price == 500_000
        assert flat_info.area == 52
        assert flat_info.flat_type == FlatType.ONE_BEDROOM.value
        assert flat_info.residential_complex == "Meridian Apartments"
        assert flat_info.floor == 2
        assert flat_info.total_floors == 12
        assert flat_info.construction_year is None
        assert flat_info.parking is None
        assert flat_info.is_rental

    def test_scrape_rental_flat_from_rental_page_success_1(self):
        """Test direct rental page scraping with the first test ID."""
        logger.info(
            f"Testing direct rental page scraping for Krisha ID: {TEST_RENTAL_KRISHA_ID_1}"
        )
        logger.info(f"URL: https://krisha.kz/a/show/{TEST_RENTAL_KRISHA_ID_1}")

        flat_info = scrape_rental_flat_from_rental_page(TEST_RENTAL_KRISHA_ID_1)

        # Fail if None
        assert flat_info is not None, (
            "Failed to scrape rental flat from rental page - returned None."
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
        assert flat_info.flat_id == TEST_RENTAL_KRISHA_ID_1
        assert flat_info.price == 1_100_000
        assert flat_info.area == 250
        assert flat_info.flat_type == FlatType.THREE_PLUS_BEDROOM.value
        assert flat_info.residential_complex == "Кокше"
        assert flat_info.floor == 3
        assert flat_info.total_floors == 3
        assert flat_info.construction_year is None
        assert flat_info.parking is None
        assert flat_info.is_rental

    def test_scrape_rental_flat_from_rental_page_success_2(self):
        """Test direct rental page scraping with the second test ID."""
        logger.info(
            f"Testing direct rental page scraping for Krisha ID: {TEST_RENTAL_KRISHA_ID_2}"
        )
        logger.info(f"URL: https://krisha.kz/a/show/{TEST_RENTAL_KRISHA_ID_2}")

        flat_info = scrape_rental_flat_from_rental_page(TEST_RENTAL_KRISHA_ID_2)

        # Fail if None
        assert flat_info is not None, (
            "Failed to scrape rental flat from rental page - returned None."
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
        assert flat_info.flat_id == TEST_RENTAL_KRISHA_ID_2
        assert flat_info.price == 500_000
        assert flat_info.area == 52
        assert flat_info.flat_type == FlatType.ONE_BEDROOM.value
        assert flat_info.residential_complex == "Meridian Apartments"
        assert flat_info.floor == 2
        assert flat_info.total_floors == 12
        assert flat_info.construction_year is None
        assert flat_info.parking is None
        assert flat_info.is_rental
