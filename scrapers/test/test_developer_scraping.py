"""
Test developer scraping from Krisha.kz complex pages.

python -m pytest scrapers/test/test_developer_scraping.py -v -s
"""

import logging
import pytest

from scrapers.src.residential_complex_scraper import fetch_developer_for_jk

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class TestDeveloperScraping:
    """Test developer name extraction from Krisha.kz complex pages."""

    def test_fetch_developer_ak_bota(self):
        """Test fetching developer for Ak Bota in Astana."""
        developer = fetch_developer_for_jk("ak-bota", city="astana")
        if developer is None:
            pytest.skip("Krisha.kz unreachable (timeout or blocked)")
        assert isinstance(developer, str)
        assert len(developer) > 0
        logger.info(f"Developer for ak-bota: {developer}")

    def test_fetch_developer_nonexistent(self):
        """Test that a nonexistent JK returns None."""
        developer = fetch_developer_for_jk("zzznonexistent999", city="almaty")
        assert developer is None
