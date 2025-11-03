from db.src.write_read_database import OrthancDB
import logging
import pytest
from datetime import datetime, timedelta
from scrapers.src.krisha_sales_scraping import scrape_and_save_jk_sales


class TestMiscQueries:
    @pytest.fixture
    def db(self):
        """Create database connection using actual database."""
        return OrthancDB("flats.db")

    def test_get_latest_sales_by_jk(self, db):
        """Test getting recent sales data (updated within 24 hours) for a residential complex."""
        jk_name = "Meridian Apartments"

        # First, scrape and save flats for sale for this JK to ensure we have recent data
        logging.info(
            f"Scraping and saving sales flats for {jk_name} to ensure we have recent data..."
        )
        saved_count = scrape_and_save_jk_sales(
            jk_name=jk_name,
            max_pages=2,  # Limit pages for faster test
            db_path="flats.db",
        )
        logging.info(f"Saved {saved_count} sales flats for {jk_name}")

        # Get recent sales data
        recent_sales = db.get_latest_sales_by_jk(jk_name)

        # Verify we get a list
        assert isinstance(recent_sales, list)
        assert len(recent_sales) >= 10

        # If we have data, verify structure
        if recent_sales:
            first_sale = recent_sales[0]
            # Check that required fields are present
            assert "flat_id" in first_sale
            assert "price" in first_sale
            assert "area" in first_sale
            assert "residential_complex" in first_sale
            assert "query_date" in first_sale
            assert "updated_at" in first_sale  # Should have updated_at field

            # Verify all sales are from the correct JK
            for sale in recent_sales:
                assert sale["residential_complex"] == jk_name
                # Verify updated_at is recent (within 24 hours)
                assert sale["updated_at"] is not None

                # Parse the updated_at timestamp and verify it's within 24 hours
                updated_at_str = sale["updated_at"]
                if isinstance(updated_at_str, str):
                    # Parse the timestamp string
                    updated_at = datetime.strptime(updated_at_str, "%Y-%m-%d %H:%M:%S")
                else:
                    updated_at = updated_at_str

                # Check if it's within the last 24 hours
                now = datetime.now()
                time_diff = now - updated_at
                assert time_diff <= timedelta(hours=24), (
                    f"Sale {sale['flat_id']} updated_at {updated_at_str} is older than 24 hours (diff: {time_diff})"
                )

            logging.info(f"Found {len(recent_sales)} recent sales for {jk_name}")
        else:
            logging.info(
                f"No recent sales data found for {jk_name} (updated within 24 hours)"
            )

        # Test with non-existent JK
        non_existent_sales = db.get_latest_sales_by_jk("Non-Existent JK")
        assert isinstance(non_existent_sales, list)
        assert len(non_existent_sales) == 0
