"""
Test rental scrapping functionality.

This module tests the rental flat scraping functionality using pytest.
"""

import sys
import os
import pytest
import logging
from typing import Optional

from common.src.flat_type import FLAT_TYPE_VALUES, FlatType

# Add the project root to Python path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', '..'))

from scrapers.src.rental_scrapping import scrape_rental_flat
from common.src.flat_info import FlatInfo

# Test Krisha ID for rental flat
# https://krisha.kz/a/show/1006270605
TEST_RENTAL_KRISHA_ID_1 = "1006270605"
TEST_RENTAL_KRISHA_ID_2 = "1006381629"
# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class TestRentalScrapping:
    """Test class for rental flat scraping functionality."""

    def test_scrape_rental_flat_success_1(self):
        """
        Test successful rental flat scraping.
        
        This test verifies that the scraper can successfully extract
        all required fields from a rental flat listing.
        """
        logger.info(f"Testing rental scrapping for Krisha ID: {TEST_RENTAL_KRISHA_ID_1}")
        logger.info(f"URL: https://krisha.kz/a/show/{TEST_RENTAL_KRISHA_ID_1}")

        # Scrape the rental flat
        flat_info = scrape_rental_flat(TEST_RENTAL_KRISHA_ID_1)

        # CRITICAL: Test should fail if scraping returns None
        assert flat_info is not None, "Failed to scrape rental flat - returned None. This indicates the scraper is not working properly."

        logger.info("Successfully scraped rental flat!")
        logger.info("=" * 50)
        logger.info("FLAT INFO FIELDS:")
        logger.info("=" * 50)

        # Check and log all FlatInfo fields
        fields_to_check = [
            ('flat_id', flat_info.flat_id),
            ('price', flat_info.price),
            ('area', flat_info.area),
            ('flat_type', flat_info.flat_type),
            ('residential_complex', flat_info.residential_complex),
            ('floor', flat_info.floor),
            ('total_floors', flat_info.total_floors),
            ('construction_year', flat_info.construction_year),
            ('parking', flat_info.parking),
            ('description', flat_info.description),
            ('is_rental', flat_info.is_rental)
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
        logger.info(f"Testing rental scrapping for Krisha ID: {TEST_RENTAL_KRISHA_ID_2}")
        logger.info(f"URL: https://krisha.kz/a/show/{TEST_RENTAL_KRISHA_ID_2}")

        # Scrape the rental flat
        flat_info = scrape_rental_flat(TEST_RENTAL_KRISHA_ID_2)

        # CRITICAL: Test should fail if scraping returns None
        assert flat_info is not None, "Failed to scrape rental flat - returned None. This indicates the scraper is not working properly."

        logger.info("Successfully scraped rental flat!")
        logger.info("=" * 50)
        logger.info("FLAT INFO FIELDS:")
        logger.info("=" * 50)

        # Check and log all FlatInfo fields
        fields_to_check = [
            ('flat_id', flat_info.flat_id),
            ('price', flat_info.price),
            ('area', flat_info.area),
            ('flat_type', flat_info.flat_type),
            ('residential_complex', flat_info.residential_complex),
            ('floor', flat_info.floor),
            ('total_floors', flat_info.total_floors),
            ('construction_year', flat_info.construction_year),
            ('parking', flat_info.parking),
            ('description', flat_info.description),
            ('is_rental', flat_info.is_rental)
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
        assert flat_info is not None, "Cannot test structure - flat_info is None. This indicates the scraper is not working properly."

        # Verify it's a FlatInfo instance
        assert isinstance(flat_info, FlatInfo), f"Expected FlatInfo instance, got {type(flat_info)}"

        # Verify field types
        assert isinstance(flat_info.flat_id, str), f"flat_id should be str, got {type(flat_info.flat_id)}"
        assert isinstance(flat_info.price, (int, float)), f"price should be int/float, got {type(flat_info.price)}"
        assert isinstance(flat_info.area, (int, float)), f"area should be int/float, got {type(flat_info.area)}"
        assert isinstance(flat_info.is_rental, bool), f"is_rental should be bool, got {type(flat_info.is_rental)}"

        logger.info("✅ FlatInfo structure validation passed!")
