"""
Performance comparison: Analytics API vs Full HTML page scraping.

Fetches 100 flat IDs from the database and times both methods to compare speed.

python -m pytest scrapers/test/test_api_vs_html_performance.py -v -s --log-cli-level=INFO
"""

import logging
import sqlite3
import time

from scrapers.src.krisha_full_page_scraping import fetch_full_html_page
from scrapers.src.krisha_sales_scraping import scrape_sales_flat_from_analytics_page

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

DB_PATH = "flats.db"
SAMPLE_SIZE = 100


def _get_flat_ids(n: int) -> list[str]:
    """Get n non-archived sales flat IDs from the database."""
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


class TestApiVsHtmlPerformance:
    """Benchmark analytics API vs full HTML page scraping."""

    def test_performance_comparison(self):
        """Time both methods on 100 flats and compare."""
        flat_ids = _get_flat_ids(SAMPLE_SIZE)
        assert len(flat_ids) == SAMPLE_SIZE, (
            f"Need {SAMPLE_SIZE} flats in DB, got {len(flat_ids)}"
        )

        logger.info(f"Benchmarking {SAMPLE_SIZE} flats...")

        # --- Analytics API ---
        api_times = []
        api_successes = 0
        api_failures = 0

        logger.info("=== Analytics API ===")
        api_total_start = time.time()
        for flat_id in flat_ids:
            start = time.time()
            result = scrape_sales_flat_from_analytics_page(flat_id)
            elapsed = time.time() - start
            api_times.append(elapsed)
            if result is not None:
                api_successes += 1
            else:
                api_failures += 1
        api_total = time.time() - api_total_start

        # --- Full HTML Page ---
        html_times = []
        html_successes = 0
        html_failures = 0

        logger.info("=== Full HTML Page ===")
        html_total_start = time.time()
        for flat_id in flat_ids:
            start = time.time()
            result = fetch_full_html_page(flat_id)
            elapsed = time.time() - start
            html_times.append(elapsed)
            if result is not None:
                html_successes += 1
            else:
                html_failures += 1
        html_total = time.time() - html_total_start

        # --- Results ---
        api_avg = sum(api_times) / len(api_times)
        html_avg = sum(html_times) / len(html_times)
        api_median = sorted(api_times)[len(api_times) // 2]
        html_median = sorted(html_times)[len(html_times) // 2]

        logger.info("\n" + "=" * 60)
        logger.info("RESULTS")
        logger.info("=" * 60)
        logger.info(f"Sample size: {SAMPLE_SIZE} flats")
        logger.info("")
        logger.info("Analytics API:")
        logger.info(f"  Total time:  {api_total:.1f}s")
        logger.info(f"  Avg/flat:    {api_avg:.3f}s")
        logger.info(f"  Median/flat: {api_median:.3f}s")
        logger.info(f"  Successes:   {api_successes}")
        logger.info(f"  Failures:    {api_failures}")
        logger.info("")
        logger.info("Full HTML Page:")
        logger.info(f"  Total time:  {html_total:.1f}s")
        logger.info(f"  Avg/flat:    {html_avg:.3f}s")
        logger.info(f"  Median/flat: {html_median:.3f}s")
        logger.info(f"  Successes:   {html_successes}")
        logger.info(f"  Failures:    {html_failures}")
        logger.info("")

        if html_total > 0 and api_total > 0:
            ratio = html_total / api_total
            logger.info(f"HTML/API ratio: {ratio:.2f}x")
            if ratio > 1:
                logger.info(f"API is {ratio:.2f}x faster than HTML")
            else:
                logger.info(f"HTML is {1 / ratio:.2f}x faster than API")
