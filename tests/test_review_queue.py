"""
Test opportunity review queue functionality.

pytest tests/test_review_queue.py -v
"""

import os
import tempfile

import pytest

from db.src.write_read_database import OrthancDB


@pytest.fixture
def db():
    """Create database connection using a temporary test database."""
    fd, temp_db_path = tempfile.mkstemp(suffix=".db")
    os.close(fd)

    db = OrthancDB(temp_db_path)

    yield db

    db.disconnect()
    if os.path.exists(temp_db_path):
        os.remove(temp_db_path)


@pytest.fixture
def db_with_opportunity(db):
    """DB with a sample opportunity seeded for testing."""
    db.connect()
    # Insert a residential complex
    db.conn.execute(
        "INSERT INTO residential_complexes (complex_id, name, city, district) "
        "VALUES (?, ?, ?, ?)",
        ("rc_1", "Test JK", "Алматы", "Бостандыкский"),
    )
    # Insert a sales flat
    db.conn.execute(
        "INSERT INTO sales_flats (flat_id, price, area, flat_type, residential_complex, "
        "floor, total_floors, description, url, query_date, city, seller_type) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, date('now'), ?, ?)",
        (
            "12345",
            30000000,
            45.0,
            "1BR",
            "Test JK",
            5,
            9,
            "Test flat",
            "https://krisha.kz/a/show/12345",
            "Алматы",
            "owner",
        ),
    )
    # Insert an opportunity analysis run
    db.conn.execute(
        "INSERT INTO opportunity_analysis (run_timestamp, rank, flat_id, residential_complex, "
        "price, area, flat_type, floor, total_floors, discount_percentage_vs_median, "
        "median_price, mean_price, min_price, max_price, sample_size, url) "
        "VALUES (datetime('now'), 1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            "12345",
            "Test JK",
            30000000,
            45.0,
            "1BR",
            5,
            9,
            25.0,
            40000000,
            38000000,
            28000000,
            50000000,
            10,
            "https://krisha.kz/a/show/12345",
        ),
    )
    db.conn.commit()
    db.disconnect()
    return db


FLAT_ID = "12345"


class TestReviewQueue:
    """Test class for opportunity review queue."""

    def test_ignore_writes_reason_to_db(self, db_with_opportunity):
        """Ignoring an opportunity writes the decision and reason to DB."""
        db = db_with_opportunity
        success = db.add_review(FLAT_ID, "ignore", "first floor, no remont")
        assert success

        review = db.get_review(FLAT_ID)
        assert review is not None
        assert review["decision"] == "ignore"
        assert review["reason"] == "first floor, no remont"

    def test_consider_writes_to_db(self, db_with_opportunity):
        """Considering an opportunity writes the decision to DB."""
        db = db_with_opportunity
        success = db.add_review(FLAT_ID, "consider")
        assert success

        review = db.get_review(FLAT_ID)
        assert review is not None
        assert review["decision"] == "consider"

    def test_ignored_flat_excluded_from_next_run(self, db_with_opportunity):
        """An ignored flat should not appear in get_top_opportunities."""
        db = db_with_opportunity

        # Before ignoring, flat should appear
        opps = db.get_top_opportunities(limit=100, ignore_exclusion_days=30)
        flat_ids = [o["flat_id"] for o in opps]
        assert FLAT_ID in flat_ids

        # Ignore the flat
        db.add_review(FLAT_ID, "ignore", "not interesting")

        # After ignoring, flat should be excluded
        opps = db.get_top_opportunities(limit=100, ignore_exclusion_days=30)
        flat_ids = [o["flat_id"] for o in opps]
        assert FLAT_ID not in flat_ids

    def test_consider_does_not_exclude(self, db_with_opportunity):
        """A considered flat should still appear in opportunities."""
        db = db_with_opportunity

        db.add_review(FLAT_ID, "consider")

        opps = db.get_top_opportunities(limit=100, ignore_exclusion_days=30)
        flat_ids = [o["flat_id"] for o in opps]
        assert FLAT_ID in flat_ids

    def test_review_history(self, db_with_opportunity):
        """Multiple reviews for the same flat are recorded and retrievable."""
        db = db_with_opportunity

        db.add_review(FLAT_ID, "consider")
        db.add_review(FLAT_ID, "ignore", "changed my mind")

        reviews = db.get_reviews_for_flat(FLAT_ID)
        assert len(reviews) == 2
        # Most recent first
        assert reviews[0]["decision"] == "ignore"
        assert reviews[1]["decision"] == "consider"

    def test_get_review_returns_none_for_unreviewed(self, db):
        """get_review returns None for a flat with no reviews."""
        review = db.get_review("nonexistent")
        assert review is None

    def test_get_ignored_flat_ids_within_window(self, db_with_opportunity):
        """get_ignored_flat_ids_within_window returns recently ignored flat IDs."""
        db = db_with_opportunity

        db.add_review(FLAT_ID, "ignore", "test")

        ignored = db.get_ignored_flat_ids_within_window(days=30)
        assert FLAT_ID in ignored

    def test_expired_ignore_does_not_exclude(self, db_with_opportunity):
        """An ignore review older than the exclusion window does not exclude the flat."""
        db = db_with_opportunity

        # Insert an ignore review backdated to 60 days ago
        db.connect()
        db.conn.execute(
            "INSERT INTO opportunity_reviews (flat_id, decision, reason, reviewed_at) "
            "VALUES (?, ?, ?, datetime('now', '-60 days'))",
            (FLAT_ID, "ignore", "old reason"),
        )
        db.conn.commit()
        db.disconnect()

        # With 30-day window, the 60-day-old ignore should not exclude
        opps = db.get_top_opportunities(limit=100, ignore_exclusion_days=30)
        flat_ids = [o["flat_id"] for o in opps]
        assert FLAT_ID in flat_ids
