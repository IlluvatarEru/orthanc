"""
Test script for currency database functionality.
"""

from db.src.write_read_database import OrthancDB
import logging
import pytest
import datetime


class TestDatabaseOperationsCurrency:
    @pytest.fixture
    def db_fx(self):
        """Create database connection using actual database."""
        return OrthancDB("flats.db")

    def test_currency_db_functionality(self, db_fx):
        """Test the currency database functionality."""

        logging.info("Testing Currency Database Functionality")
        logging.info("=" * 50)

        # Test 1: Create database and insert rates
        logging.info("1. Testing database creation and rate insertion...")

        # Insert some test rates
        now = datetime.datetime.now()
        success1 = db_fx.insert_exchange_rate("EUR", 500.0, now)
        success2 = db_fx.insert_exchange_rate("USD", 450.0, now)

        assert success1, "EUR rate insertion should succeed"
        assert success2, "USD rate insertion should succeed"

        # Test 2: Get latest rates from database
        logging.info("2. Testing rate retrieval from database...")
        eur_rate = db_fx.get_latest_rate("EUR")
        usd_rate = db_fx.get_latest_rate("USD")

        assert eur_rate == 500.0, f"Expected EUR rate 500.0, got {eur_rate}"
        assert usd_rate == 450.0, f"Expected USD rate 450.0, got {usd_rate}"

        # Test 3: Test date range queries
        logging.info("3. Testing date range queries...")
        yesterday = now - datetime.timedelta(days=1)
        tomorrow = now + datetime.timedelta(days=1)
        rates_by_date = db_fx.get_rates_by_date_range(
            "EUR", yesterday.strftime("%Y-%m-%d"), tomorrow.strftime("%Y-%m-%d")
        )

        assert len(rates_by_date) >= 1, (
            f"Expected at least 1 EUR rate in date range, got {len(rates_by_date)}"
        )

        # Test 4: Test currency list
        logging.info("4. Testing currency list...")
        currencies = db_fx.get_all_currencies()

        assert "EUR" in currencies, f"Expected EUR in currencies, got {currencies}"
        assert "USD" in currencies, f"Expected USD in currencies, got {currencies}"

        # Test 5: Test cleanup
        logging.info("5. Testing cleanup...")
        # Delete rates at the specific timestamp we inserted
        deleted_count = db_fx.delete_rate_at_timestamp(now)

        assert deleted_count >= 2, (
            f"Expected to delete at least 2 records, deleted {deleted_count}"
        )

        logging.info("Currency database functionality test completed")
