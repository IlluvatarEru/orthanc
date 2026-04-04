"""
Test full HTML page scraping functionality.

These tests hit live Krisha.kz endpoints. Tests that depend on specific flat IDs
will skip gracefully if the flat is no longer available (archived/removed).

python -m pytest scrapers/test/test_krisha_full_page_scraping.py -v -s --log-cli-level=INFO
"""

import logging

import pytest

from scrapers.src.krisha_full_page_scraping import fetch_full_html_page
from scrapers.src.krisha_sales_scraping import scrape_sales_flat_from_analytics_page

# https://krisha.kz/a/show/1009857459
TEST_KRISHA_ID = "1009857459"

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class TestFullPageScraping:
    """Test class for full HTML page scraping functionality."""

    def test_fetch_full_html_page_basic(self):
        """Test basic functionality of fetch_full_html_page."""
        result = fetch_full_html_page(TEST_KRISHA_ID)
        if result is None:
            pytest.skip(f"Flat {TEST_KRISHA_ID} no longer available on Krisha")

        logger.info("=== Full HTML Page Data ===")
        logger.info(f"  krisha_id:    {result['krisha_id']}")
        logger.info(f"  is_archived:  {result['is_archived']}")
        logger.info(f"  price:        {result['price']}")
        logger.info(f"  area:         {result['area']}")
        logger.info(f"  rooms:        {result['rooms']}")
        logger.info(f"  city:         {result['city']}")
        logger.info(f"  district:     {result['district']}")
        logger.info(f"  full_address: {result['full_address']}")
        logger.info(f"  coordinates:  {result['coordinates']}")
        logger.info(f"  complex_id:   {result['complex_id']}")
        logger.info(f"  price_per_m2: {result['price_per_m2']}")
        logger.info(f"  days_in_live: {result['days_in_live']}")
        logger.info(f"  added_at:     {result['added_at']}")
        logger.info(f"  created_at:   {result['created_at']}")

        logger.info("\n=== Characteristics (dt/dd pairs) ===")
        for key, value in result.get("characteristics", {}).items():
            logger.info(f"  {key}: {value}")

        assert result["krisha_id"] == TEST_KRISHA_ID
        assert result["characteristics"] is not None
        assert len(result["characteristics"]) > 0

    def test_characteristics_contain_expected_fields(self):
        """Test that characteristics include fields not available from the API."""
        result = fetch_full_html_page(TEST_KRISHA_ID)
        if result is None:
            pytest.skip(f"Flat {TEST_KRISHA_ID} no longer available on Krisha")

        chars = result["characteristics"]

        # These fields should be present for listing 1009857459
        expected_keys = [
            "Санузел",
            "Высота потолков",
            "Квартира меблирована",
            "Безопасность",
        ]
        for key in expected_keys:
            assert key in chars, f"Expected characteristic '{key}' not found"
            logger.info(f"  {key}: {chars[key]}")

    def test_compare_with_analytics_api(self):
        """Compare full HTML data with analytics API to show additional fields."""
        full_page_data = fetch_full_html_page(TEST_KRISHA_ID)
        api_data = scrape_sales_flat_from_analytics_page(TEST_KRISHA_ID)

        if full_page_data is None:
            pytest.skip(f"Flat {TEST_KRISHA_ID} not available via full HTML page")
        if api_data is None:
            pytest.skip(f"Flat {TEST_KRISHA_ID} not available via analytics API")

        logger.info("=== Analytics API Data (FlatInfo) ===")
        logger.info(f"  price:                {api_data.price}")
        logger.info(f"  area:                 {api_data.area}")
        logger.info(f"  flat_type:            {api_data.flat_type}")
        logger.info(f"  floor:                {api_data.floor}")
        logger.info(f"  total_floors:         {api_data.total_floors}")
        logger.info(f"  residential_complex:  {api_data.residential_complex}")
        logger.info(f"  construction_year:    {api_data.construction_year}")
        logger.info(f"  parking:              {api_data.parking}")
        logger.info(f"  city:                 {api_data.city}")
        logger.info(f"  district:             {api_data.district}")

        logger.info("\n=== Full HTML Page Convenience Fields ===")
        logger.info(f"  price:        {full_page_data['price']}")
        logger.info(f"  area:         {full_page_data['area']}")
        logger.info(f"  rooms:        {full_page_data['rooms']}")
        logger.info(f"  city:         {full_page_data['city']}")
        logger.info(f"  district:     {full_page_data['district']}")
        logger.info(f"  coordinates:  {full_page_data['coordinates']}")
        logger.info(f"  price_per_m2: {full_page_data['price_per_m2']}")
        logger.info(f"  days_in_live: {full_page_data['days_in_live']}")
        logger.info(f"  added_at:     {full_page_data['added_at']}")
        logger.info(f"  created_at:   {full_page_data['created_at']}")

        # Fields the API also covers (via regex parsing of title/description)
        api_covered_keys = {"Площадь", "Этаж", "Год постройки", "Жилой комплекс"}

        additional_fields = {
            k: v
            for k, v in full_page_data["characteristics"].items()
            if k not in api_covered_keys
        }

        logger.info("\n=== ADDITIONAL Fields (HTML only, NOT in analytics API) ===")
        for key, value in additional_fields.items():
            logger.info(f"  {key}: {value}")
        logger.info(
            f"\nTotal additional characteristic fields: {len(additional_fields)}"
        )

        # Also log top-level fields not in API
        extra_top = []
        if full_page_data.get("coordinates"):
            extra_top.append(f"  coordinates:  {full_page_data['coordinates']}")
        if full_page_data.get("price_per_m2"):
            extra_top.append(f"  price_per_m2: {full_page_data['price_per_m2']}")
        if full_page_data.get("days_in_live") is not None:
            extra_top.append(f"  days_in_live: {full_page_data['days_in_live']}")
        if full_page_data.get("added_at"):
            extra_top.append(f"  added_at:     {full_page_data['added_at']}")
        if full_page_data.get("created_at"):
            extra_top.append(f"  created_at:   {full_page_data['created_at']}")
        if full_page_data.get("rooms") is not None:
            extra_top.append(f"  rooms:        {full_page_data['rooms']}")
        if extra_top:
            logger.info("\n=== ADDITIONAL Top-level Fields (from window.data) ===")
            for line in extra_top:
                logger.info(line)

        assert len(additional_fields) > 0, (
            "Full HTML page should provide fields beyond what the analytics API offers"
        )

    def test_window_data_extraction(self):
        """Test that window.data is properly extracted and parsed."""
        result = fetch_full_html_page(TEST_KRISHA_ID)
        if result is None:
            pytest.skip(f"Flat {TEST_KRISHA_ID} no longer available on Krisha")

        assert "window_data" in result
        assert result["window_data"] is not None

        wd = result["window_data"]
        assert "advert" in wd

        advert = wd["advert"]
        assert advert.get("id") == int(TEST_KRISHA_ID)
        logger.info(f"  window.data.advert.id: {advert.get('id')}")
        logger.info(f"  window.data.advert keys: {list(advert.keys())}")
