"""
Relist detection for Krisha.kz flats.

Sellers frequently pull and re-list flats under a new flat_id.  This module
detects probable re-lists by matching on JK + type + area + price and then
comparing listing descriptions with difflib.SequenceMatcher.

Performance target: <500ms per flat on a 10k+ flat database.
"""

import logging
import os
import sqlite3
from difflib import SequenceMatcher
from typing import Optional

import toml

logger = logging.getLogger(__name__)


def _load_relist_config() -> dict:
    config_path = os.path.join(
        os.path.dirname(__file__), "..", "..", "config", "src", "config.toml"
    )
    try:
        config = toml.load(config_path)
        return config.get("relist", {})
    except Exception:
        return {}


def detect_relist(
    conn: sqlite3.Connection,
    flat_id: str,
    residential_complex: Optional[str],
    flat_type: Optional[str],
    area: float,
    price: int,
    description: str,
    table: str,
) -> Optional[dict]:
    """Detect if a new flat is a re-list of an existing disappeared flat.

    Args:
        conn: open sqlite3 connection
        flat_id: the new flat's ID
        residential_complex: JK name
        flat_type: e.g. '1BR', '2BR'
        area: square metres
        price: price in KZT
        description: full listing description text
        table: 'sales_flats' or 'rental_flats'

    Returns:
        dict with keys (flat_id, first_seen_at, similarity) of the best
        match, or None if no relist detected.
    """
    if not residential_complex or not flat_type or not description:
        return None

    cfg = _load_relist_config()
    similarity_threshold = cfg.get("similarity_threshold", 0.95)
    area_tol = cfg.get("area_tolerance_pct", 5.0) / 100.0
    price_tol = cfg.get("price_tolerance_pct", 10.0) / 100.0
    disappeared_cycles = cfg.get("disappeared_cycles", 3)
    lookback_days = cfg.get("lookback_days", 90)

    # Find the Nth most recent distinct query_date (the cutoff).
    # Flats whose latest query_date is older than this have "disappeared".
    cutoff_cursor = conn.execute(
        f"SELECT DISTINCT query_date FROM {table} "
        "ORDER BY query_date DESC LIMIT ? OFFSET ?",
        (1, disappeared_cycles),
    )
    cutoff_row = cutoff_cursor.fetchone()
    if cutoff_row is None:
        # Not enough scraping history to detect disappearances
        return None
    cutoff_date = cutoff_row[0]

    area_lo = area * (1 - area_tol)
    area_hi = area * (1 + area_tol)
    price_lo = price * (1 - price_tol)
    price_hi = price * (1 + price_tol)

    # Find candidates: same JK + type, similar area/price, disappeared,
    # seen within the lookback window, and not the same flat_id.
    # Uses one row per candidate (the most recent record).
    query = f"""
        SELECT flat_id, first_seen_at, description, MAX(query_date) as last_seen
        FROM {table}
        WHERE residential_complex = ?
          AND flat_type = ?
          AND area BETWEEN ? AND ?
          AND price BETWEEN ? AND ?
          AND flat_id != ?
          AND query_date >= date(?, '-' || ? || ' days')
        GROUP BY flat_id
        HAVING last_seen <= ?
    """
    params = (
        residential_complex,
        flat_type,
        area_lo,
        area_hi,
        price_lo,
        price_hi,
        flat_id,
        cutoff_date,
        lookback_days,
        cutoff_date,
    )

    candidates = conn.execute(query, params).fetchall()

    if not candidates:
        return None

    best_match = None
    best_score = 0.0

    for row in candidates:
        cand_flat_id = row[0]
        cand_first_seen = row[1]
        cand_desc = row[2] or ""

        if not cand_desc:
            continue

        score = SequenceMatcher(None, description, cand_desc).ratio()

        if score >= similarity_threshold and score > best_score:
            best_score = score
            best_match = {
                "flat_id": cand_flat_id,
                "first_seen_at": cand_first_seen,
                "similarity": round(score, 4),
            }

    if best_match:
        logger.info(
            f"Relist detected: {flat_id} -> {best_match['flat_id']} "
            f"(similarity={best_match['similarity']}, "
            f"first_seen={best_match['first_seen_at']})"
        )

    return best_match
