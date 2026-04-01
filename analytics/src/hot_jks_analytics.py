import statistics
from datetime import datetime, timedelta
from typing import Dict, List, Tuple

from common.src.logging_config import setup_logging
from db.src.write_read_database import OrthancDB

logger = setup_logging(__name__)


def _get_iso_week_string(dt: datetime) -> str:
    iso = dt.isocalendar()
    return f"{iso[0]}-W{iso[1]:02d}"


def compute_heat_scores(db: OrthancDB) -> Tuple[str, List[Dict]]:
    db.connect()

    # Get the latest query_date and a date ~28 days before
    cursor = db.conn.execute("SELECT MAX(query_date) FROM sales_flats")
    latest_date_str = cursor.fetchone()[0]
    if not latest_date_str:
        db.disconnect()
        return _get_iso_week_string(datetime.now()), []

    latest_date = datetime.strptime(latest_date_str, "%Y-%m-%d")
    old_date = latest_date - timedelta(days=28)
    old_date_str = old_date.strftime("%Y-%m-%d")

    week_string = _get_iso_week_string(latest_date)

    # Get all JKs with >= 5 active listings on the latest query_date
    cursor = db.conn.execute(
        """
        SELECT residential_complex, COUNT(DISTINCT flat_id) as cnt
        FROM sales_flats
        WHERE query_date = ? AND archived = 0
        GROUP BY residential_complex
        HAVING cnt >= 5
        """,
        (latest_date_str,),
    )
    qualifying_jks = [(row[0], row[1]) for row in cursor.fetchall()]

    if not qualifying_jks:
        db.disconnect()
        return week_string, []

    # Get city/district info for each JK
    jk_info = {}
    for jk_name, active_count in qualifying_jks:
        cursor = db.conn.execute(
            "SELECT city, district FROM residential_complexes WHERE name = ?",
            (jk_name,),
        )
        row = cursor.fetchone()
        jk_info[jk_name] = {
            "city": row["city"] if row else None,
            "district": row["district"] if row else None,
            "active_listings": active_count,
        }

    # Get all distinct query_dates
    cursor = db.conn.execute(
        "SELECT DISTINCT query_date FROM sales_flats ORDER BY query_date"
    )
    all_dates = [row[0] for row in cursor.fetchall()]

    # Classify dates into recent (last 2 weeks) and old (4-6 weeks ago)
    two_weeks_ago = (latest_date - timedelta(days=14)).strftime("%Y-%m-%d")
    four_weeks_ago = (latest_date - timedelta(days=42)).strftime("%Y-%m-%d")
    six_weeks_ago_cutoff = (latest_date - timedelta(days=28)).strftime("%Y-%m-%d")

    recent_dates = [d for d in all_dates if d >= two_weeks_ago]
    old_dates = [d for d in all_dates if four_weeks_ago <= d < six_weeks_ago_cutoff]

    raw_signals = {}

    for jk_name, active_count in qualifying_jks:
        signals = {}

        # Signal 1: Disappearance rate trend (weight 0.35)
        recent_disappearances = 0
        for i in range(len(recent_dates) - 1):
            d_curr = recent_dates[i]
            d_next = recent_dates[i + 1]
            cursor = db.conn.execute(
                """
                SELECT COUNT(*) FROM sales_flats
                WHERE residential_complex = ? AND query_date = ? AND flat_id NOT IN (
                    SELECT flat_id FROM sales_flats
                    WHERE query_date = ? AND residential_complex = ?
                    AND (relisted_from_flat_id IS NULL)
                )
                """,
                (jk_name, d_curr, d_next, jk_name),
            )
            # We need flat_ids on d_curr not appearing on d_next, excluding relisted
            cursor = db.conn.execute(
                """
                SELECT COUNT(*) FROM sales_flats a
                WHERE a.residential_complex = ? AND a.query_date = ?
                AND a.flat_id NOT IN (
                    SELECT flat_id FROM sales_flats
                    WHERE query_date = ? AND residential_complex = ?
                )
                AND a.flat_id NOT IN (
                    SELECT relisted_from_flat_id FROM sales_flats
                    WHERE query_date = ? AND residential_complex = ?
                    AND relisted_from_flat_id IS NOT NULL
                )
                """,
                (jk_name, d_curr, d_next, jk_name, d_next, jk_name),
            )
            recent_disappearances += cursor.fetchone()[0]

        old_disappearances = 0
        for i in range(len(old_dates) - 1):
            d_curr = old_dates[i]
            d_next = old_dates[i + 1]
            cursor = db.conn.execute(
                """
                SELECT COUNT(*) FROM sales_flats a
                WHERE a.residential_complex = ? AND a.query_date = ?
                AND a.flat_id NOT IN (
                    SELECT flat_id FROM sales_flats
                    WHERE query_date = ? AND residential_complex = ?
                )
                AND a.flat_id NOT IN (
                    SELECT relisted_from_flat_id FROM sales_flats
                    WHERE query_date = ? AND residential_complex = ?
                    AND relisted_from_flat_id IS NOT NULL
                )
                """,
                (jk_name, d_curr, d_next, jk_name, d_next, jk_name),
            )
            old_disappearances += cursor.fetchone()[0]

        if old_disappearances > 0:
            signals["disappearance_rate_trend"] = (
                recent_disappearances / old_disappearances
            )
        else:
            signals["disappearance_rate_trend"] = float(recent_disappearances)

        # Signal 2: Price/sqm momentum (weight 0.30)
        cursor = db.conn.execute(
            """
            SELECT price, area FROM sales_flats
            WHERE residential_complex = ? AND query_date = ? AND area > 0
            """,
            (jk_name, latest_date_str),
        )
        new_prices_sqm = [row[0] / row[1] for row in cursor.fetchall()]

        cursor = db.conn.execute(
            """
            SELECT price, area FROM sales_flats
            WHERE residential_complex = ? AND query_date = ? AND area > 0
            """,
            (jk_name, old_date_str),
        )
        old_prices_sqm = [row[0] / row[1] for row in cursor.fetchall()]

        # If no exact match on old_date_str, find nearest date
        if not old_prices_sqm:
            cursor = db.conn.execute(
                """
                SELECT DISTINCT query_date FROM sales_flats
                WHERE residential_complex = ? AND query_date <= ?
                ORDER BY query_date DESC LIMIT 1
                """,
                (jk_name, old_date_str),
            )
            nearest = cursor.fetchone()
            if nearest:
                cursor = db.conn.execute(
                    """
                    SELECT price, area FROM sales_flats
                    WHERE residential_complex = ? AND query_date = ? AND area > 0
                    """,
                    (jk_name, nearest[0]),
                )
                old_prices_sqm = [row[0] / row[1] for row in cursor.fetchall()]

        new_median = statistics.median(new_prices_sqm) if new_prices_sqm else 0
        old_median = statistics.median(old_prices_sqm) if old_prices_sqm else 0

        if old_median > 0:
            signals["price_sqm_momentum_pct"] = ((new_median / old_median) - 1) * 100
        else:
            signals["price_sqm_momentum_pct"] = 0.0

        # Signal 3: New listing velocity (weight 0.20)
        fourteen_days_ago = (latest_date - timedelta(days=14)).strftime("%Y-%m-%d")
        cursor = db.conn.execute(
            """
            SELECT COUNT(DISTINCT flat_id) FROM sales_flats
            WHERE residential_complex = ? AND first_seen_at >= ?
            """,
            (jk_name, fourteen_days_ago),
        )
        recent_new_listings = cursor.fetchone()[0]

        eighty_four_days_ago = (latest_date - timedelta(days=84)).strftime("%Y-%m-%d")
        cursor = db.conn.execute(
            """
            SELECT COUNT(DISTINCT flat_id) FROM sales_flats
            WHERE residential_complex = ? AND first_seen_at >= ?
            """,
            (jk_name, eighty_four_days_ago),
        )
        total_new_84d = cursor.fetchone()[0]
        avg_per_window = total_new_84d / 6.0 if total_new_84d > 0 else 0

        if avg_per_window > 0:
            signals["new_listing_velocity"] = recent_new_listings / avg_per_window
        else:
            signals["new_listing_velocity"] = float(recent_new_listings)

        # Signal 4: Days-on-market (weight 0.15)
        thirty_days_ago = (latest_date - timedelta(days=30)).strftime("%Y-%m-%d")
        # Find flats that disappeared in last 30 days
        dom_values = []
        recent_date_pairs = [
            (all_dates[i], all_dates[i + 1])
            for i in range(len(all_dates) - 1)
            if all_dates[i] >= thirty_days_ago
        ]
        for d_curr, d_next in recent_date_pairs:
            cursor = db.conn.execute(
                """
                SELECT a.flat_id, a.first_seen_at FROM sales_flats a
                WHERE a.residential_complex = ? AND a.query_date = ?
                AND a.flat_id NOT IN (
                    SELECT flat_id FROM sales_flats
                    WHERE query_date = ? AND residential_complex = ?
                )
                AND a.flat_id NOT IN (
                    SELECT relisted_from_flat_id FROM sales_flats
                    WHERE query_date = ? AND residential_complex = ?
                    AND relisted_from_flat_id IS NOT NULL
                )
                """,
                (jk_name, d_curr, d_next, jk_name, d_next, jk_name),
            )
            for row in cursor.fetchall():
                first_seen = row[1]
                if first_seen:
                    try:
                        first_seen_dt = datetime.strptime(first_seen, "%Y-%m-%d")
                        disappear_dt = datetime.strptime(d_next, "%Y-%m-%d")
                        dom = (disappear_dt - first_seen_dt).days
                        if dom >= 0:
                            dom_values.append(dom)
                    except ValueError:
                        pass

        avg_dom = statistics.mean(dom_values) if dom_values else 0
        signals["avg_days_on_market"] = avg_dom
        if avg_dom > 0:
            signals["dom_raw"] = 1.0 / avg_dom
        else:
            signals["dom_raw"] = 0.0

        # Store median price sqm for the JK
        signals["median_price_sqm"] = new_median

        raw_signals[jk_name] = signals

    # Normalize each signal to [0,1] via min-max
    signal_keys = [
        "disappearance_rate_trend",
        "price_sqm_momentum_pct",
        "new_listing_velocity",
        "dom_raw",
    ]
    mins = {}
    maxs = {}
    for key in signal_keys:
        values = [raw_signals[jk][key] for jk in raw_signals]
        mins[key] = min(values) if values else 0
        maxs[key] = max(values) if values else 0

    def normalize(val, key):
        range_ = maxs[key] - mins[key]
        if range_ == 0:
            return 0.5
        return (val - mins[key]) / range_

    # Compute final heat scores
    weights = {
        "disappearance_rate_trend": 0.35,
        "price_sqm_momentum_pct": 0.30,
        "new_listing_velocity": 0.20,
        "dom_raw": 0.15,
    }

    results = []
    for jk_name in raw_signals:
        sigs = raw_signals[jk_name]
        heat_score = sum(
            weights[key] * normalize(sigs[key], key) for key in signal_keys
        )

        results.append(
            {
                "jk_name": jk_name,
                "city": jk_info[jk_name]["city"],
                "district": jk_info[jk_name]["district"],
                "heat_score": round(heat_score, 4),
                "signals": {
                    "disappearance_rate_trend": round(
                        sigs["disappearance_rate_trend"], 4
                    ),
                    "price_sqm_momentum_pct": round(sigs["price_sqm_momentum_pct"], 2),
                    "new_listing_velocity": round(sigs["new_listing_velocity"], 4),
                    "avg_days_on_market": round(sigs["avg_days_on_market"], 1),
                },
                "active_listings": jk_info[jk_name]["active_listings"],
                "median_price_sqm": round(sigs["median_price_sqm"], 0)
                if sigs["median_price_sqm"]
                else None,
            }
        )

    results.sort(key=lambda x: x["heat_score"], reverse=True)
    db.disconnect()

    logger.info(f"Computed heat scores for {len(results)} JKs (week {week_string})")
    return week_string, results


def compute_price_trends(db: OrthancDB) -> Tuple[str, str, List[Dict]]:
    db.connect()

    # Get the three most recent distinct query_dates
    cursor = db.conn.execute(
        "SELECT DISTINCT query_date FROM sales_flats ORDER BY query_date DESC LIMIT 3"
    )
    dates = [row[0] for row in cursor.fetchall()]

    if len(dates) < 2:
        db.disconnect()
        week = _get_iso_week_string(datetime.now())
        return week, week, []

    new_date = dates[0]
    old_date = dates[1]
    third_date = dates[2] if len(dates) >= 3 else None

    new_dt = datetime.strptime(new_date, "%Y-%m-%d")
    current_week = _get_iso_week_string(new_dt)
    old_dt = datetime.strptime(old_date, "%Y-%m-%d")
    reference_week = _get_iso_week_string(old_dt)

    # Get JKs with >= 3 listings on BOTH dates
    cursor = db.conn.execute(
        """
        SELECT s1.residential_complex
        FROM (
            SELECT residential_complex, COUNT(DISTINCT flat_id) as cnt
            FROM sales_flats WHERE query_date = ? AND area > 0
            GROUP BY residential_complex HAVING cnt >= 3
        ) s1
        JOIN (
            SELECT residential_complex, COUNT(DISTINCT flat_id) as cnt
            FROM sales_flats WHERE query_date = ? AND area > 0
            GROUP BY residential_complex HAVING cnt >= 3
        ) s2 ON s1.residential_complex = s2.residential_complex
        """,
        (new_date, old_date),
    )
    qualifying_jks = [row[0] for row in cursor.fetchall()]

    results = []
    for jk_name in qualifying_jks:
        # Get city
        cursor = db.conn.execute(
            "SELECT city FROM residential_complexes WHERE name = ?",
            (jk_name,),
        )
        city_row = cursor.fetchone()
        city = city_row["city"] if city_row else None

        # For-sale: median(price/area) on new_date vs old_date
        cursor = db.conn.execute(
            "SELECT price / area as psqm FROM sales_flats WHERE residential_complex = ? AND query_date = ? AND area > 0",
            (jk_name, new_date),
        )
        new_prices = [row[0] for row in cursor.fetchall()]

        cursor = db.conn.execute(
            "SELECT price / area as psqm FROM sales_flats WHERE residential_complex = ? AND query_date = ? AND area > 0",
            (jk_name, old_date),
        )
        old_prices = [row[0] for row in cursor.fetchall()]

        new_median = statistics.median(new_prices) if new_prices else 0
        old_median = statistics.median(old_prices) if old_prices else 0

        if old_median > 0:
            for_sale_change_pct = ((new_median - old_median) / old_median) * 100
        else:
            for_sale_change_pct = 0.0

        for_sale = {
            "old_price_sqm": round(old_median, 0),
            "new_price_sqm": round(new_median, 0),
            "change_pct": round(for_sale_change_pct, 2),
        }

        # Sold: flat_ids on old_date but NOT on new_date (excluding relists)
        sold = None
        cursor = db.conn.execute(
            """
            SELECT a.flat_id, a.price, a.area FROM sales_flats a
            WHERE a.residential_complex = ? AND a.query_date = ? AND a.area > 0
            AND a.flat_id NOT IN (
                SELECT flat_id FROM sales_flats
                WHERE query_date = ? AND residential_complex = ?
            )
            AND a.flat_id NOT IN (
                SELECT relisted_from_flat_id FROM sales_flats
                WHERE query_date = ? AND residential_complex = ?
                AND relisted_from_flat_id IS NOT NULL
            )
            """,
            (jk_name, old_date, new_date, jk_name, new_date, jk_name),
        )
        sold_rows = cursor.fetchall()
        sold_prices_sqm = [row[1] / row[2] for row in sold_rows if row[2] > 0]

        # Previous sold: flat_ids on third_date but NOT on old_date
        prev_sold_prices_sqm = []
        if third_date:
            cursor = db.conn.execute(
                """
                SELECT a.flat_id, a.price, a.area FROM sales_flats a
                WHERE a.residential_complex = ? AND a.query_date = ? AND a.area > 0
                AND a.flat_id NOT IN (
                    SELECT flat_id FROM sales_flats
                    WHERE query_date = ? AND residential_complex = ?
                )
                AND a.flat_id NOT IN (
                    SELECT relisted_from_flat_id FROM sales_flats
                    WHERE query_date = ? AND residential_complex = ?
                    AND relisted_from_flat_id IS NOT NULL
                )
                """,
                (jk_name, third_date, old_date, jk_name, old_date, jk_name),
            )
            prev_rows = cursor.fetchall()
            prev_sold_prices_sqm = [row[1] / row[2] for row in prev_rows if row[2] > 0]

        if len(sold_prices_sqm) >= 1 and len(prev_sold_prices_sqm) >= 1:
            sold_median = statistics.median(sold_prices_sqm)
            prev_sold_median = statistics.median(prev_sold_prices_sqm)
            if prev_sold_median > 0:
                sold_change_pct = (
                    (sold_median - prev_sold_median) / prev_sold_median
                ) * 100
            else:
                sold_change_pct = 0.0
            sold = {
                "old_price_sqm": round(prev_sold_median, 0),
                "new_price_sqm": round(sold_median, 0),
                "change_pct": round(sold_change_pct, 2),
            }

        results.append(
            {
                "jk_name": jk_name,
                "city": city,
                "for_sale": for_sale,
                "sold": sold,
            }
        )

    db.disconnect()
    logger.info(
        f"Computed price trends for {len(results)} JKs ({old_date} -> {new_date})"
    )
    return current_week, reference_week, results
