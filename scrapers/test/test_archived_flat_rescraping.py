"""
Test that previously archived flats are re-scraped when they appear in search results.

Verifies the fix for the bug where flats appearing in Krisha search results were
skipped if they had archived=1 in the database, even though they were relisted.

pytest scrapers/test/test_archived_flat_rescraping.py -v
"""

import os
import tempfile

import pytest

from common.src.flat_info import FlatInfo
from db.src.write_read_database import OrthancDB

TEST_FLAT_ID = "1003074564"


@pytest.fixture
def db():
    fd, temp_db_path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    db = OrthancDB(temp_db_path)
    yield db
    db.disconnect()
    if os.path.exists(temp_db_path):
        os.remove(temp_db_path)


@pytest.fixture
def archived_sales_flat():
    return FlatInfo(
        flat_id=TEST_FLAT_ID,
        price=25000000,
        area=65.0,
        flat_type="2BR",
        residential_complex="Test Complex",
        floor=5,
        total_floors=10,
        construction_year=2020,
        parking=None,
        description="Test flat description",
        is_rental=False,
        archived=True,
    )


@pytest.fixture
def archived_rental_flat():
    return FlatInfo(
        flat_id=TEST_FLAT_ID,
        price=200000,
        area=65.0,
        flat_type="2BR",
        residential_complex="Test Complex",
        floor=5,
        total_floors=10,
        construction_year=2020,
        parking=None,
        description="Test rental flat description",
        is_rental=True,
        archived=True,
    )


class TestArchivedFlatRescraping:
    def test_archived_sales_flat_is_not_skipped(self, db):
        """Verify is_flat_archived is no longer used to skip flats in scrape_jk_sales."""
        # The scraper no longer calls db.is_flat_archived to filter flats,
        # so archived flats appearing in search results will be scraped.
        # We verify by checking that the scraper code collects all flat IDs
        # without filtering by archived status.
        from scrapers.src.krisha_sales_scraping import scrape_jk_sales
        import inspect

        source = inspect.getsource(scrape_jk_sales)
        assert "is_flat_archived" not in source, (
            "scrape_jk_sales should not call is_flat_archived"
        )

    def test_archived_rental_flat_is_not_skipped(self, db):
        """Verify is_flat_archived is no longer used to skip flats in scrape_jk_rentals."""
        from scrapers.src.krisha_rental_scraping import scrape_jk_rentals
        import inspect

        source = inspect.getsource(scrape_jk_rentals)
        assert "is_flat_archived" not in source, (
            "scrape_jk_rentals should not call is_flat_archived"
        )

    def test_insert_sales_flat_clears_archived_flag(self, db, archived_sales_flat):
        """If a flat was archived and is re-scraped as active, all rows get unarchived."""
        url = f"https://krisha.kz/a/show/{TEST_FLAT_ID}"
        old_date = "2025-01-01"

        # Insert the flat as archived
        success = db.insert_sales_flat(archived_sales_flat, url, old_date, "2BR")
        assert success

        # Verify it is archived
        assert db.is_flat_archived(TEST_FLAT_ID, is_rental=False)

        # Now re-insert with archived=False (simulates re-scraping a relisted flat)
        relisted_flat = FlatInfo(
            flat_id=TEST_FLAT_ID,
            price=26000000,
            area=65.0,
            flat_type="2BR",
            residential_complex="Test Complex",
            floor=5,
            total_floors=10,
            construction_year=2020,
            parking=None,
            description="Test flat description",
            is_rental=False,
            archived=False,
        )
        new_date = "2025-03-15"
        success = db.insert_sales_flat(relisted_flat, url, new_date, "2BR")
        assert success

        # The old row should also be unarchived
        assert not db.is_flat_archived(TEST_FLAT_ID, is_rental=False)

        # Verify both rows exist and are not archived
        db.connect()
        cursor = db.conn.execute(
            "SELECT query_date, archived FROM sales_flats WHERE flat_id = ? ORDER BY query_date",
            (TEST_FLAT_ID,),
        )
        rows = cursor.fetchall()
        db.disconnect()

        assert len(rows) == 2
        for row in rows:
            assert row["archived"] == 0, (
                f"Row for {row['query_date']} should have archived=0"
            )

    def test_insert_rental_flat_clears_archived_flag(self, db, archived_rental_flat):
        """If a rental flat was archived and is re-scraped as active, all rows get unarchived."""
        url = f"https://krisha.kz/a/show/{TEST_FLAT_ID}"
        old_date = "2025-01-01"

        # Insert the flat as archived
        success = db.insert_rental_flat(archived_rental_flat, url, old_date, "2BR")
        assert success

        # Verify it is archived
        assert db.is_flat_archived(TEST_FLAT_ID, is_rental=True)

        # Now re-insert with archived=False (simulates re-scraping a relisted flat)
        relisted_flat = FlatInfo(
            flat_id=TEST_FLAT_ID,
            price=210000,
            area=65.0,
            flat_type="2BR",
            residential_complex="Test Complex",
            floor=5,
            total_floors=10,
            construction_year=2020,
            parking=None,
            description="Test rental flat description",
            is_rental=True,
            archived=False,
        )
        new_date = "2025-03-15"
        success = db.insert_rental_flat(relisted_flat, url, new_date, "2BR")
        assert success

        # The old row should also be unarchived
        assert not db.is_flat_archived(TEST_FLAT_ID, is_rental=True)

        # Verify both rows exist and are not archived
        db.connect()
        cursor = db.conn.execute(
            "SELECT query_date, archived FROM rental_flats WHERE flat_id = ? ORDER BY query_date",
            (TEST_FLAT_ID,),
        )
        rows = cursor.fetchall()
        db.disconnect()

        assert len(rows) == 2
        for row in rows:
            assert row["archived"] == 0, (
                f"Row for {row['query_date']} should have archived=0"
            )

    def test_archived_flat_stays_archived_when_resaved_as_archived(
        self, db, archived_sales_flat
    ):
        """If a flat is re-scraped but still archived, the flag should remain."""
        url = f"https://krisha.kz/a/show/{TEST_FLAT_ID}"
        db.insert_sales_flat(archived_sales_flat, url, "2025-01-01", "2BR")
        assert db.is_flat_archived(TEST_FLAT_ID, is_rental=False)

        # Re-insert still archived
        db.insert_sales_flat(archived_sales_flat, url, "2025-03-15", "2BR")
        assert db.is_flat_archived(TEST_FLAT_ID, is_rental=False)
