"""
Migration: add relist detection columns to sales_flats and rental_flats.

Adds: first_seen_at, relisted_from_flat_id, relist_count
Backfills first_seen_at with MIN(query_date) per flat_id.
Creates composite indexes for relist candidate queries.

Safe to run multiple times (uses ALTER TABLE ... ADD COLUMN which is
ignored if the column already exists in SQLite via try/except).

Usage:
    python -m db.launch.migrate_relist_columns
"""

import logging
import sqlite3
import sys
import os

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

DB_PATH = os.environ.get("ORTHANC_DB_PATH", "flats.db")


def migrate(db_path: str = DB_PATH):
    conn = sqlite3.connect(db_path)

    # 1. Add columns
    columns = [
        ("first_seen_at", "DATE"),
        ("relisted_from_flat_id", "TEXT"),
        ("relist_count", "INTEGER DEFAULT 0"),
    ]
    for table in ("sales_flats", "rental_flats"):
        for col_name, col_type in columns:
            try:
                conn.execute(f"ALTER TABLE {table} ADD COLUMN {col_name} {col_type}")
                logger.info(f"Added {col_name} to {table}")
            except sqlite3.OperationalError as e:
                if "duplicate column" in str(e).lower():
                    logger.info(f"{col_name} already exists in {table}, skipping")
                else:
                    raise

    # 2. Create indexes
    for stmt in [
        "CREATE INDEX IF NOT EXISTS idx_sales_relist_candidates "
        "ON sales_flats(residential_complex, flat_type)",
        "CREATE INDEX IF NOT EXISTS idx_rental_relist_candidates "
        "ON rental_flats(residential_complex, flat_type)",
    ]:
        conn.execute(stmt)
        logger.info(f"Index ensured: {stmt.split('idx_')[1].split(' ')[0]}")

    conn.commit()

    # 3. Backfill first_seen_at where NULL
    for table in ("sales_flats", "rental_flats"):
        cursor = conn.execute(
            f"SELECT COUNT(*) FROM {table} WHERE first_seen_at IS NULL"
        )
        null_count = cursor.fetchone()[0]
        if null_count == 0:
            logger.info(f"{table}: first_seen_at already fully populated")
            continue

        logger.info(f"{table}: backfilling first_seen_at for {null_count} rows...")
        conn.execute(
            f"""
            UPDATE {table}
            SET first_seen_at = (
                SELECT MIN(t2.query_date)
                FROM {table} t2
                WHERE t2.flat_id = {table}.flat_id
            )
            WHERE first_seen_at IS NULL
            """
        )
        conn.commit()
        logger.info(f"{table}: backfill complete")

    conn.close()
    logger.info("Migration complete")


if __name__ == "__main__":
    db = sys.argv[1] if len(sys.argv) > 1 else DB_PATH
    migrate(db)
