"""
Tests for relist detection logic.
"""

import os
import tempfile
import time

import pytest

from common.src.flat_info import FlatInfo
from db.src.relist_detection import detect_relist
from db.src.table_creation import DatabaseSchema
from db.src.write_read_database import OrthancDB


@pytest.fixture
def db():
    fd, temp_path = tempfile.mkstemp(suffix=".db")
    os.close(fd)

    schema = DatabaseSchema(temp_path)
    schema.create_tables()

    odb = OrthancDB(temp_path)
    yield odb
    odb.disconnect()
    os.remove(temp_path)


def _make_flat(
    flat_id,
    jk="TestJK",
    flat_type="1BR",
    area=40.0,
    price=30000000,
    description="Beautiful flat with renovated kitchen and balcony view",
):
    return FlatInfo(
        flat_id=flat_id,
        price=price,
        area=area,
        flat_type=flat_type,
        residential_complex=jk,
        floor=5,
        total_floors=10,
        construction_year=2020,
        parking=None,
        description=description,
    )


class TestRelistDetection:
    def test_relist_detected_on_similar_description(self, db):
        """A flat with >0.95 description similarity to a disappeared flat is a relist."""
        original = _make_flat(
            "orig_001", description="Spacious 1BR apartment in TestJK near park"
        )

        # Insert original across several old scraping dates
        db.insert_sales_flat(
            original, "https://krisha.kz/a/show/orig_001", "2026-01-10", city="Алматы"
        )
        db.insert_sales_flat(
            original, "https://krisha.kz/a/show/orig_001", "2026-01-11", city="Алматы"
        )

        # Insert other flats on recent dates so the original "disappears"
        filler = _make_flat("filler_001", description="Totally different flat")
        for d in ["2026-03-27", "2026-03-28", "2026-03-29", "2026-03-30"]:
            db.insert_sales_flat(
                filler, "https://krisha.kz/a/show/filler_001", d, city="Алматы"
            )

        # New flat with near-identical description
        db.connect()
        result = detect_relist(
            conn=db.conn,
            flat_id="new_001",
            residential_complex="TestJK",
            flat_type="1BR",
            area=40.0,
            price=30000000,
            description="Spacious 1BR apartment in TestJK near park",
            table="sales_flats",
        )
        assert result is not None
        assert result["flat_id"] == "orig_001"
        assert result["similarity"] >= 0.95

    def test_no_relist_on_different_description(self, db):
        """A genuinely new flat should NOT be flagged as a relist."""
        original = _make_flat(
            "orig_002", description="Old listing with unique features and details"
        )
        db.insert_sales_flat(
            original, "https://krisha.kz/a/show/orig_002", "2026-01-10", city="Алматы"
        )

        filler = _make_flat("filler_002", description="Filler")
        for d in ["2026-03-27", "2026-03-28", "2026-03-29", "2026-03-30"]:
            db.insert_sales_flat(
                filler, "https://krisha.kz/a/show/filler_002", d, city="Алматы"
            )

        db.connect()
        result = detect_relist(
            conn=db.conn,
            flat_id="new_002",
            residential_complex="TestJK",
            flat_type="1BR",
            area=40.0,
            price=30000000,
            description="Completely different listing about another property entirely",
            table="sales_flats",
        )
        assert result is None

    def test_first_seen_at_inherited_on_relist(self, db):
        """Relist should inherit first_seen_at from original."""
        original = _make_flat(
            "orig_003", description="Nice cozy apartment centrally located"
        )
        db.insert_sales_flat(
            original, "https://krisha.kz/a/show/orig_003", "2026-01-15", city="Алматы"
        )

        filler = _make_flat("filler_003", description="Filler")
        for d in ["2026-03-27", "2026-03-28", "2026-03-29", "2026-03-30"]:
            db.insert_sales_flat(
                filler, "https://krisha.kz/a/show/filler_003", d, city="Алматы"
            )

        relisted = _make_flat(
            "relist_003", description="Nice cozy apartment centrally located"
        )
        db.insert_sales_flat(
            relisted, "https://krisha.kz/a/show/relist_003", "2026-03-30", city="Алматы"
        )

        flat = db.get_flat_info_by_id("relist_003")
        assert flat is not None
        assert flat.first_seen_at == "2026-01-15"
        assert flat.relisted_from_flat_id == "orig_003"

    def test_relist_count_incremented(self, db):
        """Original flat's relist_count should increment."""
        original = _make_flat(
            "orig_004", description="Wonderful view from the balcony of this flat"
        )
        db.insert_sales_flat(
            original, "https://krisha.kz/a/show/orig_004", "2026-01-20", city="Алматы"
        )

        filler = _make_flat("filler_004", description="Filler")
        for d in ["2026-03-27", "2026-03-28", "2026-03-29", "2026-03-30"]:
            db.insert_sales_flat(
                filler, "https://krisha.kz/a/show/filler_004", d, city="Алматы"
            )

        relisted = _make_flat(
            "relist_004", description="Wonderful view from the balcony of this flat"
        )
        db.insert_sales_flat(
            relisted, "https://krisha.kz/a/show/relist_004", "2026-03-30", city="Алматы"
        )

        orig_flat = db.get_flat_info_by_id("orig_004")
        assert orig_flat.relist_count >= 1

    def test_no_relist_when_area_mismatch(self, db):
        """Area outside tolerance should not trigger relist."""
        original = _make_flat(
            "orig_005", area=40.0, description="Same description here for matching test"
        )
        db.insert_sales_flat(
            original, "https://krisha.kz/a/show/orig_005", "2026-01-10", city="Алматы"
        )

        filler = _make_flat("filler_005", description="Filler")
        for d in ["2026-03-27", "2026-03-28", "2026-03-29", "2026-03-30"]:
            db.insert_sales_flat(
                filler, "https://krisha.kz/a/show/filler_005", d, city="Алматы"
            )

        db.connect()
        result = detect_relist(
            conn=db.conn,
            flat_id="new_005",
            residential_complex="TestJK",
            flat_type="1BR",
            area=60.0,  # way outside ±5%
            price=30000000,
            description="Same description here for matching test",
            table="sales_flats",
        )
        assert result is None

    def test_fresh_insert_gets_first_seen_at(self, db):
        """A genuinely new flat should get first_seen_at = query_date."""
        flat = _make_flat("fresh_001", description="Brand new listing")
        db.insert_sales_flat(
            flat, "https://krisha.kz/a/show/fresh_001", "2026-03-30", city="Алматы"
        )

        result = db.get_flat_info_by_id("fresh_001")
        assert result.first_seen_at == "2026-03-30"
        assert result.relisted_from_flat_id is None
        assert result.relist_count == 0

    def test_insert_performance(self, db):
        """Relist check should add <500ms per insert."""
        # Seed DB with 200 flats in the same JK
        for i in range(200):
            flat = _make_flat(
                f"perf_{i:04d}", description=f"Performance test flat number {i}"
            )
            db.insert_sales_flat(
                flat,
                f"https://krisha.kz/a/show/perf_{i:04d}",
                "2026-01-10",
                city="Алматы",
            )

        filler = _make_flat("perf_filler", description="Filler")
        for d in ["2026-03-27", "2026-03-28", "2026-03-29", "2026-03-30"]:
            db.insert_sales_flat(
                filler, "https://krisha.kz/a/show/perf_filler", d, city="Алматы"
            )

        # Time a new insert with relist check
        new_flat = _make_flat(
            "perf_new", description="A completely new and unique listing"
        )
        start = time.time()
        db.insert_sales_flat(
            new_flat, "https://krisha.kz/a/show/perf_new", "2026-03-30", city="Алматы"
        )
        elapsed = time.time() - start

        assert elapsed < 0.5, f"Insert took {elapsed:.3f}s, must be <500ms"
