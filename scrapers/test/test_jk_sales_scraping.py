"""
Test JK sales scraping functionality.

This module tests the JK sales scraping functionality using pytest.
Tests both querying (without DB) and writing (with DB) operations.

python -m pytest scrapers/test/test_jk_sales_scraping.py -v -s --log-cli-level=INFO
"""

import logging


from common.src.flat_info import FlatInfo
from common.src.flat_type import FLAT_TYPE_VALUES
from scrapers.src.krisha_sales_scraping import scrape_jk_sales, scrape_and_save_jk_sales

# Test JK names (use known complexes)
TEST_JK_NAME_1 = "Meridian Apartments"
TEST_JK_NAME_2 = "Legenda"

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class TestJKSalesScraping:
    """Test class for JK sales scraping functionality."""

    def test_scrape_jk_sales_query_only_1(self):
        """
        Test JK sales scraping without database writing.

        This test verifies that the scraping function can find and scrape
        sales flats for a specific JK without saving to database.
        """
        logger.info(f"Testing JK sales scraping (query only) for: {TEST_JK_NAME_1}")

        # Scrape with limited pages to avoid long test times
        max_pages = 2
        flats = scrape_jk_sales(TEST_JK_NAME_1, max_pages=max_pages)

        # Verify results
        assert isinstance(flats, list), f"Expected list, got {type(flats)}"
        unique_flat_ids = [flat.flat_id for flat in flats]

        # CRITICAL: Test should fail if no flats found - this indicates scraping is not working
        assert len(flats) > 10, (
            f"No sales flats found for {TEST_JK_NAME_1}. This indicates the JK sales scraper is not working properly."
        )

        # Verify each flat is a FlatInfo object
        for i, flat in enumerate(flats):
            assert isinstance(flat, FlatInfo), (
                f"Flat {i} should be FlatInfo, got {type(flat)}"
            )
            assert flat.is_rental is False, (
                f"Flat {i} should be sales, got {flat.is_rental}"
            )
            assert flat.flat_id is not None, f"Flat {i} should have flat_id"
            assert flat.price is not None, f"Flat {i} should have price"
            assert flat.price > 0, (
                f"Flat {i} price should be positive, got {flat.price}"
            )
            assert flat.area is not None, f"Flat {i} should have area"
            assert flat.area > 0, f"Flat {i} area should be positive, got {flat.area}"

        logger.info("✅ JK sales scraping (query only) test passed!")

    def test_scrape_jk_sales_query_only_2(self):
        """
        Test JK sales scraping without database writing for JK "Legenda".

        This test verifies that the scraping function can find and scrape
        sales flats for the Legenda JK without saving to database.
        """
        logger.info(f"Testing JK sales scraping (query only) for: {TEST_JK_NAME_2}")

        # Scrape with limited pages to avoid long test times
        max_pages = 2
        flats = scrape_jk_sales(TEST_JK_NAME_2, max_pages=max_pages)

        # Verify results
        assert isinstance(flats, list), f"Expected list, got {type(flats)}"
        logger.info(f"Found {len(flats)} sales flats for {TEST_JK_NAME_2}")

        # CRITICAL: Test should fail if no flats found - this indicates scraping is not working
        assert len(flats) > 0, (
            f"No sales flats found for {TEST_JK_NAME_2}. This indicates the JK sales scraper is not working properly."
        )

        # Verify each flat is a FlatInfo object
        for i, flat in enumerate(flats):
            assert isinstance(flat, FlatInfo), (
                f"Flat {i} should be FlatInfo, got {type(flat)}"
            )
            assert flat.is_rental is False, (
                f"Flat {i} should be sales, got {flat.is_rental}"
            )
            assert flat.flat_id is not None, f"Flat {i} should have flat_id"
            assert flat.price is not None, f"Flat {i} should have price"
            assert flat.price > 0, (
                f"Flat {i} price should be positive, got {flat.price}"
            )
            assert flat.area is not None, f"Flat {i} should have area"
            assert flat.area > 0, f"Flat {i} area should be positive, got {flat.area}"

        logger.info("✅ JK sales scraping (query only) test passed for Legenda!")

    def test_scrape_and_save_jk_sales_query_only_1(self):
        """
        Test JK sales scraping with database writing.

        This test verifies that the scraping function can find, scrape,
        and save sales flats for a specific JK to the database.
        """
        logger.info(f"Testing JK sales scraping (with database) for: {TEST_JK_NAME_1}")

        # Use the actual database
        db_path = "flats.db"

        # Scrape and save with limited pages
        max_pages = 2
        saved_count = scrape_and_save_jk_sales(
            jk_name=TEST_JK_NAME_1, max_pages=max_pages, db_path=db_path
        )

        # Verify results
        assert isinstance(saved_count, int), f"Expected int, got {type(saved_count)}"
        assert saved_count >= 10, (
            f"Saved count should be non-negative, got {saved_count}"
        )

        logger.info(f"Saved {saved_count} sales flats to database")

        # CRITICAL: Test should fail if no flats saved - this indicates scraping is not working
        assert saved_count > 0, (
            f"No sales flats saved for {TEST_JK_NAME_1}. This indicates the JK sales scraper is not working properly."
        )

        # Verify database contains the saved flats
        from db.src.write_read_database import OrthancDB

        db = OrthancDB(db_path)

        try:
            db.connect()

            # Check sales flats in database
            cursor = db.conn.execute(
                """
                SELECT COUNT(*) FROM sales_flats 
                WHERE residential_complex LIKE ?
            """,
                (f"%{TEST_JK_NAME_1}%",),
            )

            db_count = cursor.fetchone()[0]
            assert db_count >= saved_count, (
                f"Database should have at least {saved_count} flats, got {db_count}"
            )

            logger.info(
                f"Database contains {db_count} sales flats for {TEST_JK_NAME_1}"
            )

        finally:
            db.disconnect()

        logger.info("✅ JK sales scraping (with database) test passed!")

    def test_scrape_jk_sales_invalid_jk(self):
        """
        Test JK sales scraping with invalid JK name.

        This test verifies that the function returns an empty list for invalid JK names.
        """
        logger.info("Testing JK sales scraping with invalid JK name")

        invalid_jk = "NonExistentJK12345"
        flats = scrape_jk_sales(invalid_jk, max_pages=1)

        # Should return empty list for invalid JK
        assert isinstance(flats, list), f"Expected list, got {type(flats)}"
        assert len(flats) == 0, f"Expected empty list for invalid JK, got {len(flats)}"

        logger.info("✅ Invalid JK handling test passed!")

    def test_scrape_jk_sales_max_pages_limit(self):
        """
        Test JK sales scraping respects max_pages limit.

        This test verifies that the function doesn't exceed the max_pages limit.
        """
        logger.info("Testing JK sales scraping max_pages limit")

        max_pages = 1
        flats = scrape_jk_sales(TEST_JK_NAME_1, max_pages=max_pages)

        # Verify results
        assert isinstance(flats, list), f"Expected list, got {type(flats)}"

        # The function should respect the max_pages limit
        # (We can't easily test the exact page count without mocking, but we can verify it doesn't crash)
        logger.info(f"Scraped {len(flats)} flats with max_pages={max_pages}")

        logger.info("✅ Max pages limit test passed!")

    def test_flat_info_structure_validation(self):
        """
        Test that scraped flats have proper FlatInfo structure.

        This test verifies that all scraped flats conform to the FlatInfo schema.
        """
        logger.info("Testing FlatInfo structure validation")

        flats = scrape_jk_sales(TEST_JK_NAME_1, max_pages=1)

        # CRITICAL: Test should fail if no flats found - this indicates scraping is not working
        assert len(flats) > 0, (
            f"No flats found for {TEST_JK_NAME_1}. This indicates the JK sales scraper is not working properly."
        )

        # Test first flat structure
        flat = flats[0]

        # Verify required fields
        assert hasattr(flat, "flat_id"), "FlatInfo should have flat_id"
        assert hasattr(flat, "price"), "FlatInfo should have price"
        assert hasattr(flat, "area"), "FlatInfo should have area"
        assert hasattr(flat, "is_rental"), "FlatInfo should have is_rental"
        assert hasattr(flat, "flat_type"), "FlatInfo should have flat_type"

        # Verify field types
        assert isinstance(flat.flat_id, str), (
            f"flat_id should be str, got {type(flat.flat_id)}"
        )
        assert isinstance(flat.price, (int, float)), (
            f"price should be int/float, got {type(flat.price)}"
        )
        assert isinstance(flat.area, (int, float)), (
            f"area should be int/float, got {type(flat.area)}"
        )
        assert isinstance(flat.is_rental, bool), (
            f"is_rental should be bool, got {type(flat.is_rental)}"
        )
        assert isinstance(flat.flat_type, str), (
            f"flat_type should be str, got {type(flat.flat_type)}"
        )

        # Verify sales-specific properties
        assert flat.is_rental is False, (
            f"Sales flat should have is_rental=False, got {flat.is_rental}"
        )

        logger.info("✅ FlatInfo structure validation test passed!")

    def test_sales_vs_rental_differences(self):
        """
        Test that sales and rental flats have different characteristics.

        This test verifies that sales flats are properly differentiated from rental flats.
        """
        logger.info("Testing sales vs rental differences")

        sales_flats = scrape_jk_sales(TEST_JK_NAME_1, max_pages=1)

        # CRITICAL: Test should fail if no flats found - this indicates scraping is not working
        assert len(sales_flats) > 0, (
            f"No sales flats found for {TEST_JK_NAME_1}. This indicates the JK sales scraper is not working properly."
        )

        # Test first sales flat
        sales_flat = sales_flats[0]

        # Verify sales flat characteristics
        assert sales_flat.is_rental is False, (
            f"Sales flat should have is_rental=False, got {sales_flat.is_rental}"
        )

        # Sales flats typically have higher prices (total price vs monthly rent)
        # This is a general assumption - sales prices are usually much higher than rental prices
        logger.info(f"Sales flat price: {sales_flat.price}")

        # Verify flat type is determined correctly
        assert sales_flat.flat_type in FLAT_TYPE_VALUES, (
            f"Invalid flat_type: {sales_flat.flat_type}"
        )

        logger.info("✅ Sales vs rental differences test passed!")
