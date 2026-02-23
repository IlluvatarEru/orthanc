"""
Sales scraping module for fetching individual sales flat information.

This module provides functionality to scrape sales flat details from Krisha.kz
using a single flat ID and return a FlatInfo object.
"""

import requests
import re
import json
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional, List
from datetime import datetime
from bs4 import BeautifulSoup
from common.src.flat_info import FlatInfo
from scrapers.src.utils import (
    extract_price,
    extract_area,
    extract_residential_complex,
    extract_floor_info,
    extract_construction_year,
    extract_parking_info,
    extract_description,
    determine_flat_type_from_text,
    normalize_flat_type_enum,
    extract_jk_from_description,
    extract_additional_info_from_description,
    get_flat_urls_from_search_page,
    extract_flat_id_from_url,
    fetch_url,
    get_city_url_slug,
)


def scrape_sales_flat_from_sale_page(krisha_id: str) -> Optional[FlatInfo]:
    """
    Scrape sales flat information directly from the main sales page using BeautifulSoup.

    :param krisha_id: str, Krisha.kz flat ID (e.g., "12345678")
    :return: Optional[FlatInfo], flat information object or None if failed
    """
    try:
        url = f"https://krisha.kz/a/show/{krisha_id}"

        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
            "Accept-Encoding": "gzip, deflate",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
        }

        response = fetch_url(url, headers=headers, timeout=15)

        # Parse HTML with BeautifulSoup
        soup = BeautifulSoup(response.content, "html.parser")

        # Use the existing extract_sales_info function
        return extract_sales_info(soup, krisha_id, url)

    except requests.RequestException as e:
        logging.error(
            f"Request error scraping sales flat {krisha_id} from sale page (after retries): {e}"
        )
        return None
    except Exception as e:
        logging.error(
            f"Unexpected error scraping sales flat {krisha_id} from sale page: {e}"
        )
        return None


def scrape_sales_flat_from_analytics_page_with_failover_to_sale_page(
    krisha_id: str,
) -> Optional[FlatInfo]:
    """
    Scrape sales flat information with failover from analytics API to direct page scraping.

    :param krisha_id: str, Krisha.kz flat ID (e.g., "12345678")
    :return: Optional[FlatInfo], flat information object or None if both methods fail
    """
    logging.info(f"Attempting to scrape sales flat {krisha_id} with failover...")

    # Try analytics API first (faster and more reliable)
    try:
        logging.info(f"Trying analytics API for flat {krisha_id}...")
        flat_info = scrape_sales_flat_from_analytics_page(krisha_id)
        if flat_info is not None:
            logging.info(
                f"✅ Successfully scraped flat {krisha_id} using analytics API (archived: {flat_info.archived})"
            )
            return flat_info
        else:
            logging.warning(f"Analytics API returned None for flat {krisha_id}")
    except Exception as e:
        logging.warning(f"Analytics API failed for flat {krisha_id}: {e}")

    # Fallback to direct page scraping
    try:
        logging.info(f"Trying direct page scraping for flat {krisha_id}...")
        flat_info = scrape_sales_flat_from_sale_page(krisha_id)
        if flat_info is not None:
            logging.info(
                f"✅ Successfully scraped flat {krisha_id} using direct page scraping (archived: {flat_info.archived})"
            )
            return flat_info
        else:
            logging.warning(f"Direct page scraping returned None for flat {krisha_id}")
    except Exception as e:
        logging.warning(f"Direct page scraping failed for flat {krisha_id}: {e}")

    # Both methods failed
    logging.error(f"❌ Both scraping methods failed for flat {krisha_id}")
    return None


def scrape_sales_flat_from_analytics_page(
    krisha_id: str,
) -> Optional[FlatInfo]:
    """
    Fetch sales flat information via Krisha mobile analytics JSON and build FlatInfo.
    :param krisha_id: str, Krisha.kz flat ID (e.g., "12345678")
    :return: Optional[FlatInfo], flat information object or None if failed
    """
    try:
        api_url = f"https://m.krisha.kz/analytics/aPriceAnalysis/?id={krisha_id}"

        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
            "Accept": "application/json, text/plain, */*",
            "Referer": f"https://m.krisha.kz/a/show/{krisha_id}",
            "Origin": "https://m.krisha.kz",
        }

        response = fetch_url(api_url, headers=headers, timeout=15)

        if response.status_code == 204 or not response.text.strip():
            logging.warning(f"Analytics API returned no content for flat {krisha_id}")
            return None

        try:
            data = response.json()
        except Exception:
            data = json.loads(response.text)

        advert = data.get("advert", {})

        # Check if flat is archived (storage is in advert object)
        storage = advert.get("storage", "")
        is_archived = storage == "archive"

        # Price (total sales price) - extract from advert.price HTML
        price_html = advert.get("price", "")
        price_digits = re.sub(r"[^\d]", "", price_html)
        price = int(price_digits) if price_digits else None
        if price is None:
            logging.warning(f"No price found for sales flat {krisha_id}")
            return None

        # Area from title (e.g., "250 м²") or description
        title = advert.get("title", "")
        description = advert.get("description", "") or "No description available"
        area = None
        m_area = re.search(r"(\d+(?:[\.,]\d+)?)\s*м²", title)
        if m_area:
            area = float(m_area.group(1).replace(",", "."))
        else:
            m_area = re.search(r"(\d+(?:[\.,]\d+)?)\s*м²", description)
            if m_area:
                area = float(m_area.group(1).replace(",", "."))
        if area is None:
            logging.warning(f"No area found in title for sales flat {krisha_id}")
            return None

        # Floors from title (e.g., "3/9 этаж")
        floor = None
        total_floors = None
        m_floor = re.search(r"(\d+)\s*/\s*(\d+)\s*этаж", title)
        if m_floor:
            floor = int(m_floor.group(1))
            total_floors = int(m_floor.group(2))

        # description already set above
        residential_complex = extract_jk_from_description(description)
        extra = extract_additional_info_from_description(description)
        if not residential_complex:
            residential_complex = extra.get("residential_complex")

        flat_type = determine_flat_type_from_text(title, description, area)

        # City from API response (e.g. "Астана", "Алматы")
        api_city = advert.get("city")

        flat_info = FlatInfo(
            flat_id=str(krisha_id),
            price=int(price),
            area=area,
            flat_type=normalize_flat_type_enum(flat_type),
            residential_complex=residential_complex,
            floor=floor,
            total_floors=total_floors,
            construction_year=extra.get("construction_year"),
            parking=extra.get("parking"),
            description=description,
            is_rental=False,  # Sales flat
            archived=is_archived,
            city=api_city,
        )

        return flat_info
    except requests.RequestException as e:
        logging.error(f"Request error scraping sales flat {krisha_id}: {e}")
        return None
    except Exception as e:
        logging.error(f"Unexpected error scraping sales flat {krisha_id}: {e}")
        return None


def extract_sales_info(
    soup: BeautifulSoup, flat_id: str, url: str
) -> Optional[FlatInfo]:
    """
    Extract sales flat information from parsed HTML.

    :param soup: BeautifulSoup, parsed HTML content
    :param flat_id: str, flat ID
    :param url: str, original URL
    :return: Optional[FlatInfo], extracted flat information
    """
    try:
        # Extract price (sale price)
        price = extract_price(soup)
        if not price:
            logging.warning(f"Could not extract price for sales flat {flat_id}")
            return None

        # Extract area
        area = extract_area(soup)
        if not area:
            logging.warning(f"Could not extract area for sales flat {flat_id}")
            return None

        # Extract residential complex
        residential_complex = extract_residential_complex(soup)

        # Extract floor information
        floor, total_floors = extract_floor_info(soup)

        # Extract construction year
        construction_year = extract_construction_year(soup)

        # Extract parking information
        parking = extract_parking_info(soup)

        # Extract description
        description = extract_description(soup)

        # Extract title from page for better flat type determination
        title_elem = soup.select_one("h1, .offer__title, .title")
        title = title_elem.get_text(strip=True) if title_elem else ""

        # Determine flat type using text analysis (same as analytics API)
        flat_type = determine_flat_type_from_text(title, description, area)
        flat_type = normalize_flat_type_enum(flat_type)

        # Check if flat is archived by looking for the archived label
        is_archived = False
        archived_label = soup.select_one(
            "span.paid-labels__item.paid-labels__item--red"
        )
        if archived_label and archived_label.get_text(strip=True) == "В архиве":
            is_archived = True
            logging.info(f"Flat {flat_id} is marked as archived in HTML")

        # Create FlatInfo object
        flat_info = FlatInfo(
            flat_id=flat_id,
            price=price,
            area=area,
            flat_type=flat_type,
            residential_complex=residential_complex,
            floor=floor,
            total_floors=total_floors,
            construction_year=construction_year,
            parking=parking,
            description=description,
            is_rental=False,  # This is a sales flat
            archived=is_archived,
        )

        return flat_info

    except Exception as e:
        logging.error(f"Error extracting sales flat information: {e}")
        return None


def scrape_jk_sales(
    jk_name: str, max_pages: int = 10, db_path: str = "flats.db", city: str = "almaty"
) -> List[FlatInfo]:
    """
    Scrape all sales flats for a specific residential complex (JK).

    :param jk_name: str, name of the residential complex
    :param max_pages: int, maximum number of pages to scrape (default: 10)
    :param db_path: str, path to database file to check archived status
    :param city: str, city name for URL construction (default: "almaty")
    :return: List[FlatInfo], list of scraped sales flats (with archived status in FlatInfo)
    """
    logging.info(f"Starting JK sales scraping for: {jk_name}")
    logging.info(f"Max pages: {max_pages}")

    all_flats = []
    from scrapers.src.utils import throttle

    # Search for the complex to get its ID
    from scrapers.src.residential_complex_scraper import search_complex_by_name

    complex_info = search_complex_by_name(jk_name)

    if not complex_info or not complex_info.get("complex_id"):
        logging.error(f"Could not find complex ID for: {jk_name}")
        return []

    complex_id = complex_info["complex_id"]
    logging.info(f"Found complex ID: {complex_id}")

    # Initialize database connection once for checking archived status
    from db.src.write_read_database import OrthancDB

    db = OrthancDB(db_path)

    # Scrape each page
    for page in range(1, max_pages + 1):
        logging.info(f"Scraping page {page} for {jk_name}")

        # Construct search URL for this page
        city_slug = get_city_url_slug(city)
        search_url = f"https://krisha.kz/prodazha/kvartiry/{city_slug}/?das[map.complex]={complex_id}&page={page}"
        logging.info(search_url)

        # Get flat URLs from this page
        flat_urls = get_flat_urls_from_search_page(search_url)

        if not flat_urls:
            logging.info(f"No flats found on page {page}, stopping pagination")
            break

        logging.info(f"Found {len(flat_urls)} flats on page {page}")

        # Filter out archived flats first (main thread, uses DB)
        flat_ids_to_scrape = []
        for flat_url in flat_urls:
            flat_id = extract_flat_id_from_url(flat_url)
            if not flat_id:
                continue
            if db.is_flat_archived(flat_id, is_rental=False):
                logging.info(f"Skipping archived sales flat: {flat_id}")
                continue
            flat_ids_to_scrape.append(flat_id)

        # Scrape flats concurrently (adaptive throttle controls worker count)
        current_workers = throttle.max_workers
        logging.info(
            f"Scraping {len(flat_ids_to_scrape)} flats with {current_workers} workers"
        )
        with ThreadPoolExecutor(max_workers=current_workers) as executor:
            futures = {
                executor.submit(
                    scrape_sales_flat_from_analytics_page_with_failover_to_sale_page,
                    flat_id,
                ): flat_id
                for flat_id in flat_ids_to_scrape
            }

            for future in as_completed(futures):
                flat_id = futures[future]
                try:
                    flat_info = future.result()
                    if flat_info:
                        all_flats.append(flat_info)
                        logging.info(
                            f"Successfully scraped sales flat: {flat_id} (archived: {flat_info.archived})"
                        )
                    else:
                        logging.error(f"Failed to scrape sales flat: {flat_id}")
                except Exception as e:
                    logging.error(f"Error scraping flat {flat_id}: {e}")

    logging.info(
        f"Completed JK sales scraping for {jk_name}. Total flats: {len(all_flats)}"
    )
    return all_flats


def check_if_sales_flat_is_archived(flat_id: str) -> bool:
    """
    Check if a sales flat is archived by querying the analytics API.

    :param flat_id: str, Krisha.kz flat ID
    :return: bool, True if archived, False otherwise (or if check fails)
    """
    try:
        api_url = f"https://m.krisha.kz/analytics/aPriceAnalysis/?id={flat_id}"

        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
            "Accept": "application/json, text/plain, */*",
            "Referer": f"https://m.krisha.kz/a/show/{flat_id}",
            "Origin": "https://m.krisha.kz",
        }

        response = fetch_url(api_url, headers=headers, timeout=10)

        data = response.json()

        # Storage is nested in advert object
        advert = data.get("advert", {})
        storage = advert.get("storage", "")
        return storage == "archive"
    except Exception as e:
        # asssume that if we fail to find it, it's because it's so old it's archived
        logging.warning(f"Could not check archived status for flat {flat_id}: {e}")
        return True


def scrape_and_save_jk_sales(
    jk_name: str, max_pages: int = 10, db_path: str = "flats.db", city: str = "almaty"
) -> int:
    """
    Scrape and save all sales flats for a specific residential complex (JK).
    Also checks existing non-archived flats to see if they're now archived.

    :param jk_name: str, name of the residential complex
    :param max_pages: int, maximum number of pages to scrape (default: 10)
    :param db_path: str, path to database file
    :param city: str, city name for URL construction (default: "almaty")
    :return: int, number of flats saved to database
    """
    logging.info(f"Starting JK sales scraping and saving for: {jk_name}")

    # Get existing non-archived flat IDs before scraping
    from db.src.write_read_database import OrthancDB

    db = OrthancDB(db_path)
    existing_flat_ids = db.get_non_archived_flat_ids_for_jk(jk_name, is_rental=False)

    # Scrape flats
    flats: list[FlatInfo] = scrape_jk_sales(jk_name, max_pages, db_path, city)

    logging.info(f"Initializing database connection to: {db_path}")
    saved_count = 0

    # Get scraped flat IDs
    scraped_flat_ids = {flat_info.flat_id for flat_info in flats}

    # Save scraped flats
    for flat_info in flats:
        # Prefer city from API response (per-flat), fall back to JK-level city
        flat_city = flat_info.city or city
        success = db.insert_sales_flat(
            flat_info=flat_info,
            url=f"https://krisha.kz/a/show/{flat_info.flat_id}",
            query_date=datetime.now().strftime("%Y-%m-%d"),
            flat_type=flat_info.flat_type,
            city=flat_city,
        )

        if success:
            saved_count += 1
            archived_status = " (archived)" if flat_info.archived else ""
            logging.info(f"Saved sales flat: {flat_info.flat_id}{archived_status}")
        else:
            logging.error(f"Failed to save sales flat: {flat_info.flat_id}")

    # Check existing non-archived flats that weren't in the scraped results.
    # Skip if the scrape returned 0 flats -- Krisha may be blocking us,
    # and checking individual flats would just burn retries on a blocked IP.
    if flats:
        flats_to_check = [
            flat_id for flat_id in existing_flat_ids if flat_id not in scraped_flat_ids
        ]

        if flats_to_check:
            logging.info(
                f"Checking {len(flats_to_check)} existing non-archived flats for archived status..."
            )
            archived_count = 0
            for flat_id in flats_to_check:
                if check_if_sales_flat_is_archived(flat_id):
                    if db.mark_flat_as_archived(flat_id, is_rental=False):
                        archived_count += 1
                        logging.info(f"Marked sales flat {flat_id} as archived")
            logging.info(f"Marked {archived_count} sales flats as archived")
    elif existing_flat_ids:
        logging.info(
            f"Skipping archived check for {len(existing_flat_ids)} existing flats "
            f"(scrape returned 0 results, Krisha may be blocking us)"
        )

    logging.info(
        f"Completed JK sales scraping and saving for {jk_name}. Saved: {saved_count}/{len(flats)}"
    )
    return saved_count
