"""
Test hot JKs analytics computation.

pytest analytics/test/test_hot_jks_analytics.py -v
"""

import os
import tempfile
from datetime import datetime, timedelta

import pytest

from analytics.src.hot_jks_analytics import compute_heat_scores, compute_price_trends
from common.src.flat_info import FlatInfo
from db.src.write_read_database import OrthancDB


def _insert_sales_flat(
    db,
    flat_id,
    jk_name,
    price,
    area,
    query_date,
    first_seen_at=None,
    relisted_from=None,
    flat_type="2BR",
):
    flat = FlatInfo(
        flat_id,
        price,
        area,
        flat_type,
        jk_name,
        5,
        10,
        2020,
        "Yes",
        f"Test flat {flat_id}",
        False,
    )
    if first_seen_at:
        flat.first_seen_at = first_seen_at
    if relisted_from:
        flat.relisted_from_flat_id = relisted_from
    db.insert_sales_flat(
        flat,
        f"https://krisha.kz/a/show/{flat_id}",
        query_date,
        flat_type,
    )


class TestComputeHeatScores:
    @pytest.fixture
    def db_with_data(self):
        fd, temp_db_path = tempfile.mkstemp(suffix=".db")
        os.close(fd)
        db = OrthancDB(temp_db_path)

        today = datetime.now()
        dates = []
        # Create query dates spanning 6 weeks back
        for weeks_back in range(7):
            d = today - timedelta(weeks=weeks_back)
            dates.append(d.strftime("%Y-%m-%d"))
        dates.reverse()  # oldest first

        # Register a JK
        db.connect()
        db.conn.execute(
            "INSERT OR IGNORE INTO residential_complexes (complex_id, name, city, district) "
            "VALUES (?, ?, ?, ?)",
            ("jk001", "TestJK", "Almaty", "Bostandyk"),
        )
        db.conn.execute(
            "INSERT OR IGNORE INTO residential_complexes (complex_id, name, city, district) "
            "VALUES (?, ?, ?, ?)",
            ("jk002", "SmallJK", "Almaty", "Medeu"),
        )
        db.conn.commit()
        db.disconnect()

        # Insert 8 flats for TestJK on each date (>= 5 threshold)
        for date_str in dates:
            for i in range(8):
                first_seen = (today - timedelta(days=10)).strftime("%Y-%m-%d")
                _insert_sales_flat(
                    db,
                    f"flat_{date_str}_{i}",
                    "TestJK",
                    25000000 + i * 500000,
                    50.0 + i * 2,
                    date_str,
                    first_seen_at=first_seen,
                )

        # Insert 3 flats for SmallJK (below threshold of 5)
        latest_date = dates[-1]
        for i in range(3):
            _insert_sales_flat(
                db,
                f"small_{i}",
                "SmallJK",
                20000000,
                40.0,
                latest_date,
            )

        yield db, temp_db_path

        db.disconnect()
        if os.path.exists(temp_db_path):
            os.remove(temp_db_path)

    def test_jks_below_threshold_excluded(self, db_with_data):
        db, _ = db_with_data
        week, scores = compute_heat_scores(db)

        jk_names = [s["jk_name"] for s in scores]
        assert "SmallJK" not in jk_names
        assert "TestJK" in jk_names

    def test_heat_score_range(self, db_with_data):
        db, _ = db_with_data
        week, scores = compute_heat_scores(db)

        assert len(scores) > 0
        for s in scores:
            assert 0 <= s["heat_score"] <= 1, (
                f"heat_score {s['heat_score']} out of range for {s['jk_name']}"
            )

    def test_signals_present(self, db_with_data):
        db, _ = db_with_data
        week, scores = compute_heat_scores(db)

        for s in scores:
            assert "signals" in s
            signals = s["signals"]
            assert "disappearance_rate_trend" in signals
            assert "price_sqm_momentum_pct" in signals
            assert "new_listing_velocity" in signals
            assert "avg_days_on_market" in signals

    def test_week_string_format(self, db_with_data):
        db, _ = db_with_data
        week, scores = compute_heat_scores(db)

        assert week is not None
        assert "-W" in week

    def test_save_and_retrieve(self, db_with_data):
        db, _ = db_with_data
        week, scores = compute_heat_scores(db)

        db.save_heat_scores(scores, week)
        retrieved = db.get_hot_jks(week=week)

        assert len(retrieved) == len(scores)
        assert retrieved[0]["jk_name"] == scores[0]["jk_name"]
        assert "signals" in retrieved[0]


class TestComputePriceTrends:
    @pytest.fixture
    def db_with_trends_data(self):
        fd, temp_db_path = tempfile.mkstemp(suffix=".db")
        os.close(fd)
        db = OrthancDB(temp_db_path)

        today = datetime.now()
        new_date = today.strftime("%Y-%m-%d")
        old_date = (today - timedelta(days=3)).strftime("%Y-%m-%d")
        third_date = (today - timedelta(days=6)).strftime("%Y-%m-%d")

        # Register JK
        db.connect()
        db.conn.execute(
            "INSERT OR IGNORE INTO residential_complexes (complex_id, name, city) "
            "VALUES (?, ?, ?)",
            ("jk001", "TrendJK", "Almaty"),
        )
        db.conn.commit()
        db.disconnect()

        # Insert flats: old prices = 400k/sqm, new prices = 440k/sqm (10% increase)
        for i in range(5):
            _insert_sales_flat(
                db,
                f"old_flat_{i}",
                "TrendJK",
                20000000,
                50.0,  # 400k/sqm
                old_date,
            )
            _insert_sales_flat(
                db,
                f"new_flat_{i}",
                "TrendJK",
                22000000,
                50.0,  # 440k/sqm
                new_date,
            )

        # Insert on third_date too
        for i in range(5):
            _insert_sales_flat(
                db,
                f"third_flat_{i}",
                "TrendJK",
                19000000,
                50.0,  # 380k/sqm
                third_date,
            )

        yield db, temp_db_path

        db.disconnect()
        if os.path.exists(temp_db_path):
            os.remove(temp_db_path)

    def test_price_trends_change_pct(self, db_with_trends_data):
        db, _ = db_with_trends_data
        current_week, ref_week, trends = compute_price_trends(db)

        assert len(trends) > 0
        trend = trends[0]
        assert trend["jk_name"] == "TrendJK"

        # For sale: old = 400k, new = 440k -> +10%
        assert abs(trend["for_sale"]["change_pct"] - 10.0) < 0.1

    def test_price_trends_structure(self, db_with_trends_data):
        db, _ = db_with_trends_data
        current_week, ref_week, trends = compute_price_trends(db)

        assert current_week is not None
        assert ref_week is not None
        for t in trends:
            assert "jk_name" in t
            assert "for_sale" in t
            assert "old_price_sqm" in t["for_sale"]
            assert "new_price_sqm" in t["for_sale"]
            assert "change_pct" in t["for_sale"]

    def test_save_and_retrieve_trends(self, db_with_trends_data):
        db, _ = db_with_trends_data
        current_week, ref_week, trends = compute_price_trends(db)

        db.save_price_trends(trends, current_week)
        result = db.get_price_trends(week=current_week)

        assert result["week"] == current_week
        assert len(result["jks"]) == len(trends)
