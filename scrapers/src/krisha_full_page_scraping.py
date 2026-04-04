"""
Full HTML page scraping module for extracting detailed listing characteristics.

The mobile analytics API (m.krisha.kz/analytics/aPriceAnalysis/) provides limited
fields. The full HTML page at krisha.kz/a/show/{id} contains ~15 additional
structured fields: building type, condition, bathroom, ceiling height, furniture,
security, coordinates, price/m², days on market, etc.
"""

import json
import re
import logging
from typing import Optional

import requests
from bs4 import BeautifulSoup

from scrapers.src.utils import fetch_url

logger = logging.getLogger(__name__)

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Accept-Encoding": "gzip, deflate",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
}


def _extract_window_data(soup: BeautifulSoup) -> Optional[dict]:
    """Extract and parse the window.data JavaScript object from the page.

    :param soup: BeautifulSoup, parsed HTML content
    :return: Optional[dict], parsed window.data object or None
    """
    for script in soup.find_all("script"):
        text = script.string or ""
        match = re.search(r"window\.data\s*=\s*(\{.*?\});\s*(?:\n|$)", text, re.DOTALL)
        if match:
            json_str = match.group(1)
            try:
                return json.loads(json_str)
            except json.JSONDecodeError:
                # Try cleaning common JS-to-JSON issues
                cleaned = json_str.rstrip(";")
                try:
                    return json.loads(cleaned)
                except json.JSONDecodeError as e:
                    logger.warning(f"Failed to parse window.data JSON: {e}")
    return None


def _extract_characteristics(soup: BeautifulSoup) -> dict:
    """Extract all dt/dd key-value pairs from the listing page.

    :param soup: BeautifulSoup, parsed HTML content
    :return: dict, label->value pairs from dt/dd elements
    """
    characteristics = {}
    for dt in soup.find_all("dt"):
        label = dt.get_text(strip=True)
        dd = dt.find_next_sibling("dd")
        if dd and label:
            value = dd.get_text(strip=True)
            characteristics[label] = value
    return characteristics


def _build_result(
    krisha_id: str,
    window_data: Optional[dict],
    characteristics: dict,
) -> dict:
    """Assemble the result dict from extracted data.

    :param krisha_id: str, listing ID
    :param window_data: Optional[dict], parsed window.data object
    :param characteristics: dict, dt/dd pairs
    :return: dict, complete result
    """
    advert = {}
    adverts_0 = {}
    if window_data:
        advert = window_data.get("advert", {})
        adverts_list = window_data.get("adverts", [])
        if adverts_list:
            adverts_0 = adverts_list[0]

    # City and district
    full_address = adverts_0.get("fullAddress", "")
    city = adverts_0.get("city")
    district = None
    district_match = re.search(r"(\S+\s+р-н)", full_address)
    if district_match:
        district = district_match.group(1)

    # Archived status
    storage = advert.get("storage", "")
    is_archived = storage == "archive"

    # Coordinates
    map_data = advert.get("map", {}) or {}
    coordinates = None
    if map_data.get("lat") and map_data.get("lon"):
        coordinates = {"lat": map_data["lat"], "lon": map_data["lon"]}

    return {
        "krisha_id": krisha_id,
        "characteristics": characteristics,
        "window_data": window_data,
        "description_full": adverts_0.get("description", ""),
        "is_archived": is_archived,
        "price": advert.get("price"),
        "area": float(advert.get("square")) if advert.get("square") else None,
        "rooms": advert.get("rooms"),
        "city": city,
        "district": district,
        "full_address": full_address,
        "coordinates": coordinates,
        "complex_id": str(advert.get("complexId")) if advert.get("complexId") else None,
        "price_per_m2": adverts_0.get("priceM2"),
        "days_in_live": adverts_0.get("daysInLive"),
        "added_at": adverts_0.get("addedAt"),
        "created_at": adverts_0.get("createdAt"),
    }


def fetch_full_html_page(krisha_id: str) -> Optional[dict]:
    """Fetch and parse the full HTML page from Krisha.kz to extract ALL structured
    characteristics not available from the mobile analytics API.

    :param krisha_id: str, Krisha.kz flat ID (e.g., "1009857459")
    :return: Optional[dict], dictionary containing all extracted data, or None if
             the page could not be fetched/parsed. Keys include:
             - "krisha_id": str
             - "characteristics": dict of label->value pairs from dt/dd elements
             - "window_data": dict of structured data from window.data
             - "description_full": str
             - "is_archived": bool
             - "price": Optional[int]
             - "area": Optional[float]
             - "rooms": Optional[int]
             - "city": Optional[str]
             - "district": Optional[str]
             - "full_address": Optional[str]
             - "coordinates": Optional[dict] with "lat" and "lon"
             - "complex_id": Optional[str]
             - "price_per_m2": Optional[int]
             - "days_in_live": Optional[int]
             - "added_at": Optional[str]
             - "created_at": Optional[str]
    """
    try:
        url = f"https://krisha.kz/a/show/{krisha_id}"
        response = fetch_url(url, headers=_HEADERS, timeout=15)
        soup = BeautifulSoup(response.content, "html.parser")

        window_data = _extract_window_data(soup)
        characteristics = _extract_characteristics(soup)

        if not window_data and not characteristics:
            logger.warning(f"No data extracted from full page for flat {krisha_id}")
            return None

        return _build_result(krisha_id, window_data, characteristics)

    except requests.RequestException as e:
        logger.error(f"Request error fetching full page for flat {krisha_id}: {e}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error fetching full page for flat {krisha_id}: {e}")
        return None
