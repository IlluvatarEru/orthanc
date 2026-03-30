"""
Tests for flat condition extraction and remont penalty in opportunity scoring.

python -m pytest tests/test_condition_extraction.py -v -s --log-cli-level=INFO
"""

import os
import tempfile

import pytest

from scrapers.src.utils import extract_condition_from_description
from common.src.flat_info import FlatInfo
from db.src.write_read_database import OrthancDB
from db.src.table_creation import DatabaseSchema
from analytics.src.jk_sales_analytics import JKAnalytics


# Flat IDs from acceptance criteria
FLAT_ID_NEEDS_REMONT = "1002014502"
FLAT_ID_RENOVATED = "1010781758"


# ---------------------------------------------------------------------------
# Unit tests for keyword extraction
# ---------------------------------------------------------------------------


class TestExtractCondition:
    """Test extract_condition_from_description for each keyword."""

    @pytest.mark.parametrize(
        "desc,expected",
        [
            ("Квартира в черновая отделке", "needs_remont"),
            ("Продается под штукатурку", "needs_remont"),
            ("Без ремонта, нужна отделка", "needs_remont"),
            ("Требует ремонта, цена снижена", "needs_remont"),
            ("Чистовая отделка, заходи и живи", "renovated"),
            ("Состояние хорошее", "renovated"),
            ("Сделан евроремонт", "renovated"),
            ("Евро ремонт, мебель", "renovated"),
            ("Квартира под ключ", "renovated"),
            ("Свежий ремонт 2024 года", "renovated"),
            ("Продается 2-комнатная квартира", "unknown"),
            ("", "unknown"),
            (None, "unknown"),
        ],
    )
    def test_keyword_matching(self, desc, expected):
        assert extract_condition_from_description(desc) == expected

    def test_case_insensitive(self):
        assert extract_condition_from_description("ЧЕРНОВАЯ отделка") == "needs_remont"
        assert extract_condition_from_description("ЕВРОРЕМОНТ") == "renovated"

    def test_needs_remont_takes_priority(self):
        """If both keywords appear, needs_remont wins (checked first)."""
        desc = "Черновая отделка, рядом квартиры с евроремонтом"
        assert extract_condition_from_description(desc) == "needs_remont"


# ---------------------------------------------------------------------------
# Database integration tests
# ---------------------------------------------------------------------------


@pytest.fixture
def tmp_db():
    """Create a temporary database with schema for testing."""
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    schema = DatabaseSchema(path)
    schema.initialize_database()
    yield path
    os.unlink(path)


class TestConditionInDB:
    """Test condition column in sales_flats and rental_flats."""

    def test_insert_sales_flat_with_condition(self, tmp_db):
        db = OrthancDB(tmp_db)
        flat = FlatInfo(
            flat_id="test_1",
            price=30_000_000,
            area=60.0,
            flat_type="2BR",
            residential_complex="TestJK",
            floor=3,
            total_floors=10,
            construction_year=2020,
            parking=None,
            description="Черновая отделка",
            is_rental=False,
            condition="needs_remont",
        )
        db.insert_sales_flat(flat, "https://example.com", "2026-01-01", city="Алматы")

        db.connect()
        row = db.conn.execute(
            "SELECT condition FROM sales_flats WHERE flat_id = ?", ("test_1",)
        ).fetchone()
        db.disconnect()
        assert row["condition"] == "needs_remont"

    def test_insert_rental_flat_with_condition(self, tmp_db):
        db = OrthancDB(tmp_db)
        flat = FlatInfo(
            flat_id="test_2",
            price=200_000,
            area=45.0,
            flat_type="1BR",
            residential_complex="TestJK",
            floor=5,
            total_floors=12,
            construction_year=2021,
            parking=None,
            description="Евроремонт, мебель",
            is_rental=True,
            condition="renovated",
        )
        db.insert_rental_flat(flat, "https://example.com", "2026-01-01")

        db.connect()
        row = db.conn.execute(
            "SELECT condition FROM rental_flats WHERE flat_id = ?", ("test_2",)
        ).fetchone()
        db.disconnect()
        assert row["condition"] == "renovated"

    def test_backfill_condition(self, tmp_db):
        """Test that backfill_condition updates NULL condition rows."""
        db = OrthancDB(tmp_db)

        # Insert a flat without condition (simulate pre-migration row)
        db.connect()
        db.conn.execute(
            """INSERT INTO sales_flats
               (flat_id, price, area, flat_type, residential_complex,
                description, url, query_date)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                "backfill_1",
                25_000_000,
                50.0,
                "1BR",
                "TestJK",
                "Квартира без ремонта",
                "https://example.com",
                "2026-01-01",
            ),
        )
        db.conn.commit()
        db.disconnect()

        updated = db.backfill_condition(extract_condition_from_description)
        assert updated >= 1

        db.connect()
        row = db.conn.execute(
            "SELECT condition FROM sales_flats WHERE flat_id = ?", ("backfill_1",)
        ).fetchone()
        db.disconnect()
        assert row["condition"] == "needs_remont"


# ---------------------------------------------------------------------------
# Opportunity scoring penalty tests
# ---------------------------------------------------------------------------


class TestRemontPenalty:
    """Test that remont penalty suppresses false positives."""

    def _setup_jk_data(self, db_path):
        """Insert test data: 5 renovated flats at ~40M and 1 needs_remont at 30M."""
        db = OrthancDB(db_path)
        jk = "PenaltyTestJK"
        date = "2026-01-15"

        # 5 renovated flats around 40M (similar area ~60 sqm)
        for i, price in enumerate(
            [39_000_000, 40_000_000, 41_000_000, 40_500_000, 39_500_000]
        ):
            flat = FlatInfo(
                flat_id=f"pen_{i}",
                price=price,
                area=60.0,
                flat_type="2BR",
                residential_complex=jk,
                floor=i + 2,
                total_floors=12,
                construction_year=2022,
                parking=None,
                description="Чистовая отделка, мебель",
                is_rental=False,
                condition="renovated",
                city="Алматы",
            )
            db.insert_sales_flat(flat, f"https://example.com/{i}", date, city="Алматы")

        # 1 needs_remont flat at 30M -- looks like 25% discount but needs 6M renovation
        needs_remont_flat = FlatInfo(
            flat_id="pen_rough",
            price=30_000_000,
            area=60.0,
            flat_type="2BR",
            residential_complex=jk,
            floor=1,
            total_floors=12,
            construction_year=2022,
            parking=None,
            description="Черновая отделка, без ремонта",
            is_rental=False,
            condition="needs_remont",
            city="Алматы",
        )
        db.insert_sales_flat(
            needs_remont_flat, "https://example.com/rough", date, city="Алматы"
        )
        return jk

    def test_penalty_suppresses_false_positive(self, tmp_db):
        """
        A 30M needs_remont flat with 60 sqm area:
        - Median of bucket ~40M
        - Without penalty: discount = (40M - 30M)/40M = 25% -> surfaces as opportunity
        - With penalty (100K/sqm): effective = 30M + 6M = 36M, discount = (40M - 36M)/40M = 10%
        - At 15% threshold, it should NOT surface.
        """
        jk = self._setup_jk_data(tmp_db)
        analytics = JKAnalytics(tmp_db)

        analysis = analytics.analyse_jk_for_sales(jk, 0.15, city="Алматы")
        opportunities = analysis["current_market"].opportunities

        # Collect all opportunity flat IDs
        opp_flat_ids = []
        for flat_type, opps in opportunities.items():
            for opp in opps:
                opp_flat_ids.append(opp.flat_info.flat_id)

        assert "pen_rough" not in opp_flat_ids, (
            "needs_remont flat should be suppressed by remont penalty"
        )

    def test_genuine_deal_still_surfaces(self, tmp_db):
        """A renovated flat that is genuinely cheap should still surface."""
        db = OrthancDB(tmp_db)
        jk = self._setup_jk_data(tmp_db)

        # Add a genuinely cheap renovated flat
        cheap = FlatInfo(
            flat_id="pen_cheap",
            price=32_000_000,
            area=60.0,
            flat_type="2BR",
            residential_complex=jk,
            floor=4,
            total_floors=12,
            construction_year=2022,
            parking=None,
            description="Чистовая отделка, срочная продажа",
            is_rental=False,
            condition="renovated",
            city="Алматы",
        )
        db.insert_sales_flat(
            cheap, "https://example.com/cheap", "2026-01-15", city="Алматы"
        )

        analytics = JKAnalytics(tmp_db)
        analysis = analytics.analyse_jk_for_sales(jk, 0.15, city="Алматы")
        opportunities = analysis["current_market"].opportunities

        opp_flat_ids = []
        for flat_type, opps in opportunities.items():
            for opp in opps:
                opp_flat_ids.append(opp.flat_info.flat_id)

        assert "pen_cheap" in opp_flat_ids, (
            "Genuinely cheap renovated flat should still surface as opportunity"
        )


# ---------------------------------------------------------------------------
# Live DB condition tests (require production database with the specified flats)
# ---------------------------------------------------------------------------


class TestLiveConditionParsed:
    """Test condition extraction on real flats from the production database."""

    @pytest.fixture(autouse=True)
    def _check_db(self):
        if not os.path.exists("flats.db"):
            pytest.skip("Production database flats.db not found")

    def test_needs_remont_flat_exists_in_db(self):
        """Verify we can find and classify a needs_remont flat in the DB."""
        db = OrthancDB("flats.db")
        db.connect()
        row = db.conn.execute(
            "SELECT flat_id, description FROM sales_flats "
            "WHERE description LIKE '%черновая%' OR description LIKE '%без ремонта%' "
            "OR description LIKE '%под штукатурку%' OR description LIKE '%требует ремонта%' "
            "ORDER BY query_date DESC LIMIT 1"
        ).fetchone()
        db.disconnect()

        if row is None:
            pytest.skip("No needs_remont flat found in database")

        condition = extract_condition_from_description(row["description"])
        assert condition == "needs_remont", (
            f"Flat {row['flat_id']} has remont keyword but extracted {condition}"
        )

    def test_renovated_flat_exists_in_db(self):
        """Verify we can find and classify a renovated flat in the DB."""
        db = OrthancDB("flats.db")
        db.connect()
        row = db.conn.execute(
            "SELECT flat_id, description FROM sales_flats "
            "WHERE description LIKE '%свежий ремонт%' OR description LIKE '%евроремонт%' "
            "OR description LIKE '%чистовая%' OR description LIKE '%под ключ%' "
            "ORDER BY query_date DESC LIMIT 1"
        ).fetchone()
        db.disconnect()

        if row is None:
            pytest.skip("No renovated flat found in database")

        condition = extract_condition_from_description(row["description"])
        assert condition == "renovated", (
            f"Flat {row['flat_id']} has renovated keyword but extracted {condition}"
        )

    def test_specific_flat_renovated(self):
        """Flat 1010781758 (specified in acceptance criteria) should be renovated."""
        db = OrthancDB("flats.db")
        db.connect()
        row = db.conn.execute(
            "SELECT description FROM sales_flats WHERE flat_id = ? ORDER BY query_date DESC LIMIT 1",
            (FLAT_ID_RENOVATED,),
        ).fetchone()
        db.disconnect()

        if row is None:
            pytest.skip(f"Flat {FLAT_ID_RENOVATED} not found in database")

        condition = extract_condition_from_description(row["description"])
        assert condition == "renovated", (
            f"Expected renovated for flat {FLAT_ID_RENOVATED}, got {condition}"
        )


# ---------------------------------------------------------------------------
# Full suite smoke test
# ---------------------------------------------------------------------------


class TestFullSuitePasses:
    """Verify the full test suite passes."""

    def test_full_suite_passes(self):
        import subprocess
        import sys

        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "pytest",
                "tests/",
                "-x",
                "--tb=short",
                "-q",
                "--deselect=tests/test_condition_extraction.py::TestFullSuitePasses",
            ],
            capture_output=True,
            text=True,
            cwd="/root/orthanc",
        )
        assert result.returncode == 0, (
            f"Test suite failed:\n{result.stdout}\n{result.stderr}"
        )
