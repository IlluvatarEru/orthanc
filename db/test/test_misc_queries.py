"""
Test miscellaneous database queries.

pytest db/test/test_misc_queries.py -v
"""

import pytest
import tempfile
import os
import logging
from datetime import datetime, timedelta

from db.src.write_read_database import OrthancDB
from common.src.flat_info import FlatInfo


class TestMiscQueries:
    """Test class for miscellaneous database queries."""

    @pytest.fixture
    def db_fx(self):
        """Create database connection using a temporary test database with pre-populated data."""
        # Create a temporary database file for testing
        fd, temp_db_path = tempfile.mkstemp(suffix=".db")
        os.close(fd)

        db = OrthancDB(temp_db_path)

        # Insert test sales flats for testing
        jk_name = "Test Complex"
        query_date = datetime.now().strftime("%Y-%m-%d")

        test_flats = [
            FlatInfo(
                f"test_sale_{i}",
                25000000 + i * 1000000,  # 25M - 35M
                50.0 + i * 5,
                "2BR",
                jk_name,
                i + 1,
                10,
                2020,
                "Yes",
                f"Test flat {i}",
                False,
            )
            for i in range(10)
        ]

        for flat in test_flats:
            db.insert_sales_flat(
                flat,
                f"https://krisha.kz/a/show/{flat.flat_id}",
                query_date,
                flat.flat_type,
            )

        yield db

        # Cleanup: remove the temporary database after test
        db.disconnect()
        if os.path.exists(temp_db_path):
            os.remove(temp_db_path)

    def test_get_latest_sales_by_jk(self, db_fx):
        """Test getting recent sales data for a residential complex."""
        jk_name = "Test Complex"

        # Get recent sales data
        recent_sales = db_fx.get_latest_sales_by_jk(jk_name)

        # Verify we get a list
        assert isinstance(recent_sales, list)
        assert len(recent_sales) == 10, f"Expected 10 sales, got {len(recent_sales)}"

        # Verify structure
        first_sale = recent_sales[0]
        assert "flat_id" in first_sale, "flat_id field missing"
        assert "price" in first_sale, "price field missing"
        assert "area" in first_sale, "area field missing"
        assert "residential_complex" in first_sale, "residential_complex field missing"
        assert "query_date" in first_sale, "query_date field missing"
        assert "updated_at" in first_sale, "updated_at field missing"

        # Verify all sales are from the correct JK
        for sale in recent_sales:
            assert sale["residential_complex"] == jk_name
            assert sale["updated_at"] is not None

            # Parse the updated_at timestamp and verify it's within 24 hours
            updated_at_str = sale["updated_at"]
            if isinstance(updated_at_str, str):
                updated_at = datetime.strptime(updated_at_str, "%Y-%m-%d %H:%M:%S")
            else:
                updated_at = updated_at_str

            now = datetime.now()
            time_diff = now - updated_at
            assert time_diff <= timedelta(hours=24), (
                f"Sale {sale['flat_id']} updated_at is older than 24 hours (diff: {time_diff})"
            )

        logging.info(f"Found {len(recent_sales)} recent sales for {jk_name}")

    def test_get_latest_sales_by_nonexistent_jk(self, db_fx):
        """Test getting sales data for non-existent JK returns empty list."""
        non_existent_sales = db_fx.get_latest_sales_by_jk("Non-Existent JK")
        assert isinstance(non_existent_sales, list)
        assert len(non_existent_sales) == 0, "Non-existent JK should return empty list"

    def test_sales_data_fields_are_valid(self, db_fx):
        """Test that sales data fields have valid values."""
        jk_name = "Test Complex"
        recent_sales = db_fx.get_latest_sales_by_jk(jk_name)

        for sale in recent_sales:
            # Price should be positive
            assert sale["price"] > 0, f"Price should be positive: {sale['price']}"

            # Area should be positive
            assert sale["area"] > 0, f"Area should be positive: {sale['area']}"

            # Flat ID should be non-empty
            assert sale["flat_id"], "Flat ID should not be empty"


class TestJKPriceTrend:
    """Test class for JK price trend queries."""

    @pytest.fixture
    def db_fx(self):
        """Create database with multi-date sales data for price trend testing."""
        fd, temp_db_path = tempfile.mkstemp(suffix=".db")
        os.close(fd)
        db = OrthancDB(temp_db_path)

        jk_name = "Trend JK"
        # Insert flats across two query dates so we can detect sold flats
        date1 = "2025-03-01"
        date2 = "2025-03-08"

        # Date 1: flats 1-5
        for i in range(1, 6):
            flat = FlatInfo(
                f"trend_{i}",
                20000000 + i * 1000000,
                50.0 + i * 5,
                "2BR",
                jk_name,
                i,
                10,
                2020,
                "Yes",
                f"Flat {i}",
                False,
            )
            db.insert_sales_flat(
                flat, f"https://krisha.kz/a/show/trend_{i}", date1, flat.flat_type
            )

        # Date 2: flats 3-7 (1,2 disappeared = sold; 6,7 are new)
        for i in range(3, 8):
            flat = FlatInfo(
                f"trend_{i}",
                20000000 + i * 1000000,
                50.0 + i * 5,
                "2BR",
                jk_name,
                i,
                10,
                2020,
                "Yes",
                f"Flat {i}",
                False,
            )
            db.insert_sales_flat(
                flat, f"https://krisha.kz/a/show/trend_{i}", date2, flat.flat_type
            )

        yield db

        db.disconnect()
        if os.path.exists(temp_db_path):
            os.remove(temp_db_path)

    def test_price_trend_returns_weekly_data(self, db_fx):
        """Test that price trend returns aggregated weekly data."""
        result = db_fx.get_jk_price_trend("Trend JK")
        assert isinstance(result, list)
        assert len(result) > 0

        for entry in result:
            assert "week" in entry
            assert "for_sale_median_sqm" in entry
            assert "for_sale_mean_sqm" in entry
            assert "sold_median_sqm" in entry
            assert "sold_mean_sqm" in entry

    def test_price_trend_has_for_sale_data(self, db_fx):
        """Test that for-sale series has values."""
        result = db_fx.get_jk_price_trend("Trend JK")
        has_for_sale = any(e["for_sale_median_sqm"] is not None for e in result)
        assert has_for_sale, "Should have for-sale data"

    def test_price_trend_has_sold_data(self, db_fx):
        """Test that sold series has values (flats 1,2 disappeared)."""
        result = db_fx.get_jk_price_trend("Trend JK")
        has_sold = any(e["sold_median_sqm"] is not None for e in result)
        assert has_sold, "Should have sold data (flats 1,2 disappeared between dates)"

    def test_price_trend_nonexistent_jk(self, db_fx):
        """Test that nonexistent JK returns empty list."""
        result = db_fx.get_jk_price_trend("Nonexistent")
        assert result == []


class TestOpportunityFrequency:
    """Test class for opportunity frequency queries."""

    @pytest.fixture
    def db_fx(self):
        """Create database with opportunity analysis data."""
        fd, temp_db_path = tempfile.mkstemp(suffix=".db")
        os.close(fd)
        db = OrthancDB(temp_db_path)

        # Insert some opportunity analysis rows
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        opportunities = [
            {
                "rank": i,
                "flat_id": f"opp_{i}",
                "residential_complex": "Opp JK",
                "price": 20000000 + i * 1000000,
                "area": 60.0,
                "flat_type": "2BR",
                "floor": 3,
                "total_floors": 10,
                "construction_year": 2021,
                "parking": "Yes",
                "discount_percentage_vs_median": 0.15 + i * 0.02,
                "median_price": 25000000,
                "mean_price": 24000000,
                "min_price": 20000000,
                "max_price": 30000000,
                "sample_size": 10,
                "query_date": datetime.now().strftime("%Y-%m-%d"),
                "url": f"https://krisha.kz/a/show/opp_{i}",
                "description": f"Opp flat {i}",
            }
            for i in range(1, 6)
        ]
        db.insert_opportunity_analysis_batch(opportunities, now)

        yield db

        db.disconnect()
        if os.path.exists(temp_db_path):
            os.remove(temp_db_path)

    def test_opportunity_frequency_counts(self, db_fx):
        """Test that opportunity frequency returns correct count."""
        count = db_fx.get_opportunity_frequency("Opp JK", days=90, min_discount=0.15)
        assert count == 5

    def test_opportunity_frequency_with_higher_threshold(self, db_fx):
        """Test filtering by higher discount threshold."""
        count = db_fx.get_opportunity_frequency("Opp JK", days=90, min_discount=0.20)
        # Discounts: 0.17, 0.19, 0.21, 0.23, 0.25 -- 3 are >= 0.20
        assert count == 3

    def test_opportunity_frequency_nonexistent_jk(self, db_fx):
        """Test nonexistent JK returns 0."""
        count = db_fx.get_opportunity_frequency("Nonexistent", days=90)
        assert count == 0
