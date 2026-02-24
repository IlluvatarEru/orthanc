"""
Clean performance test for full HTML page scraping only.

Waits 3 hours for rate limit cooldown, then fetches 100 flats from the HTML page
with per-request timestamps and success/failure logging.

Usage:
    nohup python -m pytest scrapers/test/test_html_performance_clean.py -v -s --log-cli-level=INFO > html_perf.out 2>&1 &
"""

import logging
import sqlite3
import time
from datetime import datetime

from scrapers.src.krisha_full_page_scraping import fetch_full_html_page

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

DB_PATH = "flats.db"
SAMPLE_SIZE = 100
COOLDOWN_HOURS = 3


def _get_flat_ids(n: int) -> list[str]:
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.execute(
        "SELECT DISTINCT flat_id FROM sales_flats "
        "WHERE archived = 0 OR archived IS NULL "
        "LIMIT ?",
        (n,),
    )
    ids = [row[0] for row in cursor.fetchall()]
    conn.close()
    return ids


class TestHtmlPerformanceClean:
    def test_html_scraping_performance(self):
        flat_ids = _get_flat_ids(SAMPLE_SIZE)
        assert len(flat_ids) == SAMPLE_SIZE

        # Cooldown to avoid carry-over rate limiting
        logger.info(
            f"Waiting {COOLDOWN_HOURS}h for rate limit cooldown... "
            f"will resume at {datetime.now().strftime('%H:%M')} + {COOLDOWN_HOURS}h"
        )
        time.sleep(COOLDOWN_HOURS * 3600)
        logger.info("Cooldown complete. Starting HTML scraping benchmark.")

        results = []
        consecutive_failures = 0

        for i, flat_id in enumerate(flat_ids):
            start = time.time()
            data = fetch_full_html_page(flat_id)
            elapsed = time.time() - start
            success = data is not None

            if success:
                n_chars = len(data.get("characteristics", {}))
                consecutive_failures = 0
            else:
                n_chars = 0
                consecutive_failures += 1

            results.append({"flat_id": flat_id, "success": success, "time": elapsed})

            logger.info(
                f"[{i + 1:3d}/{SAMPLE_SIZE}] {flat_id}  "
                f"{'OK' if success else 'FAIL'}  "
                f"{elapsed:5.2f}s  "
                f"chars={n_chars}"
            )

            # Stop early if blocked (10 consecutive failures)
            if consecutive_failures >= 10:
                logger.warning(
                    f"Stopping early: {consecutive_failures} consecutive failures"
                )
                break

        # Summary
        successes = [r for r in results if r["success"]]
        failures = [r for r in results if not r["success"]]
        total_time = sum(r["time"] for r in results)

        logger.info("\n" + "=" * 60)
        logger.info("RESULTS")
        logger.info("=" * 60)
        logger.info(f"Attempted:  {len(results)}/{SAMPLE_SIZE}")
        logger.info(f"Successes:  {len(successes)}")
        logger.info(f"Failures:   {len(failures)}")
        logger.info(f"Total time: {total_time:.1f}s")

        if successes:
            s_times = [r["time"] for r in successes]
            logger.info(f"Avg success time:    {sum(s_times) / len(s_times):.3f}s")
            logger.info(
                f"Median success time: {sorted(s_times)[len(s_times) // 2]:.3f}s"
            )
            logger.info(f"Min success time:    {min(s_times):.3f}s")
            logger.info(f"Max success time:    {max(s_times):.3f}s")

        if failures:
            first_fail_idx = next(i for i, r in enumerate(results) if not r["success"])
            logger.info(f"First failure at request #{first_fail_idx + 1}")

        # Log the per-request timeline
        logger.info("\n=== Timeline ===")
        for i, r in enumerate(results):
            marker = "+" if r["success"] else "X"
            logger.info(f"  {marker} [{i + 1:3d}] {r['flat_id']} {r['time']:5.2f}s")
