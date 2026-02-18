"""
Utility functions for Krisha.kz scraping.

This module contains shared utility functions used by both rental and sales scrapers
to avoid code duplication.
"""

import requests
import re
import logging
import time
import functools
import threading
import os
from typing import Optional, List
from bs4 import BeautifulSoup
from common.src.flat_type import FlatType

# --- Shared HTTP session and rate limiter ---
_session: Optional[requests.Session] = None
_session_lock = threading.Lock()
_rate_lock = threading.Lock()
_last_request_time = 0.0


def _load_scraping_config() -> dict:
    """Load scraping configuration from config.toml."""
    try:
        import toml

        config_path = os.path.join(
            os.path.dirname(__file__), "..", "..", "config", "src", "config.toml"
        )
        config = toml.load(config_path)
        return config.get("scraping", {})
    except Exception:
        return {}


def get_scraping_config() -> dict:
    """
    Get scraping configuration with defaults.

    :return: dict with keys: concurrent_workers, delay_between_flat_requests,
             delay_between_requests, retry_attempts, max_pages_per_query
    """
    config = _load_scraping_config()
    return {
        "concurrent_workers": config.get("concurrent_workers", 3),
        "delay_between_flat_requests": config.get("delay_between_flat_requests", 0.3),
        "delay_between_requests": config.get("delay_between_requests", 2.0),
        "retry_attempts": config.get("retry_attempts", 3),
        "max_pages_per_query": config.get("max_pages_per_query", 10),
    }


def get_session() -> requests.Session:
    """
    Get or create a shared requests.Session for TCP connection reuse.
    Thread-safe via double-checked locking.

    :return: requests.Session
    """
    global _session
    if _session is None:
        with _session_lock:
            if _session is None:
                _session = requests.Session()
                _session.headers.update(
                    {
                        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/123.0.0.0 Safari/537.36",
                        "Accept-Encoding": "gzip, deflate",
                        "Connection": "keep-alive",
                    }
                )
    return _session


def retry_on_failure(max_retries: int = 3, base_delay: float = 2.0):
    """
    Decorator that retries a function on requests.RequestException.
    Uses exponential backoff: base_delay * 2^attempt (2s, 4s, 8s by default).
    Backs off 60s on HTTP 429 (rate limit).

    :param max_retries: int, maximum number of retry attempts
    :param base_delay: float, base delay in seconds between retries
    """

    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except requests.exceptions.HTTPError as e:
                    last_exception = e
                    if e.response is not None and e.response.status_code == 429:
                        delay = 60
                        logging.warning(
                            f"Rate limited (429) in {func.__name__}, "
                            f"waiting {delay}s before retry {attempt + 1}/{max_retries}"
                        )
                    elif attempt < max_retries:
                        delay = base_delay * (2**attempt)
                        logging.warning(
                            f"HTTP error in {func.__name__}: {e}, "
                            f"retrying in {delay:.0f}s ({attempt + 1}/{max_retries})"
                        )
                    else:
                        break
                    time.sleep(delay)
                except requests.RequestException as e:
                    last_exception = e
                    if attempt < max_retries:
                        delay = base_delay * (2**attempt)
                        logging.warning(
                            f"Request error in {func.__name__}: {e}, "
                            f"retrying in {delay:.0f}s ({attempt + 1}/{max_retries})"
                        )
                        time.sleep(delay)
                    else:
                        break
            logging.error(
                f"All {max_retries} retries exhausted for {func.__name__}: {last_exception}"
            )
            raise last_exception

        return wrapper

    return decorator


@retry_on_failure(max_retries=3, base_delay=2.0)
def fetch_url(url: str, headers: dict = None, timeout: int = 15) -> requests.Response:
    """
    Fetch a URL with automatic retry on transient failures.
    Uses a shared Session for TCP connection reuse and a thread-safe rate
    limiter to enforce minimum delay between requests.

    :param url: str, URL to fetch
    :param headers: dict, optional HTTP headers
    :param timeout: int, request timeout in seconds
    :return: requests.Response object
    :raises: requests.RequestException after all retries exhausted
    """
    global _last_request_time

    # Thread-safe rate limiting
    min_interval = get_scraping_config()["delay_between_flat_requests"]
    with _rate_lock:
        now = time.time()
        elapsed = now - _last_request_time
        if elapsed < min_interval:
            time.sleep(min_interval - elapsed)
        _last_request_time = time.time()

    session = get_session()
    response = session.get(url, headers=headers, timeout=timeout)
    response.raise_for_status()
    return response


# City name to Krisha.kz URL slug mapping
CITY_URL_SLUGS = {
    "almaty": "almaty",
    "astana": "astana",
    "shymkent": "shymkent",
    "караганда": "karaganda",
    "karaganda": "karaganda",
    "актау": "aktau",
    "aktau": "aktau",
    "атырау": "atyrau",
    "atyrau": "atyrau",
    "алматы": "almaty",
    "астана": "astana",
    "шымкент": "shymkent",
}


def get_city_url_slug(city: str) -> str:
    """
    Convert a city name to a Krisha.kz URL slug.

    :param city: str, city name (e.g., "almaty", "Алматы", "Astana")
    :return: str, URL slug (e.g., "almaty", "astana")
    """
    if not city:
        return "almaty"
    return CITY_URL_SLUGS.get(city.lower(), city.lower())


def extract_price(soup: BeautifulSoup) -> Optional[int]:
    """Extract price from the page."""
    try:
        # Look for price in various formats - updated selectors for current Krisha.kz structure
        price_selectors = [
            ".offer__price",
            ".price",
            '[data-testid="price"]',
            ".offer__sidebar .offer__price",
            ".offer__price-value",
            ".price-value",
            ".offer__price-value",
            ".offer__price .price-value",
            ".offer__price-value .price-value",
            # Look for price in title or main content
            "h1 .offer__price",
            ".offer__title .offer__price",
            # Look for price patterns in text
            ".offer__price-text",
            ".price-text",
        ]

        for selector in price_selectors:
            price_elem = soup.select_one(selector)
            if price_elem:
                price_text = price_elem.get_text(strip=True)
                logging.info(
                    f"Found price element with selector '{selector}': '{price_text}'"
                )

                # Extract numeric value - handle various formats
                # Remove common currency symbols and text, but keep digits and spaces
                clean_text = re.sub(r"[^\d\s\xa0]", "", price_text)
                # Match digits with any kind of space separator (regular or non-breaking)
                price_match = re.search(r"(\d+(?:[\s\xa0]*\d+)*)", clean_text)
                if price_match:
                    # Replace both regular spaces and non-breaking spaces
                    price_str = (
                        price_match.group(1).replace(" ", "").replace("\xa0", "")
                    )
                    if price_str:
                        return int(price_str)

        # If selectors don't work, try to find price in the page text
        logging.info("Trying to find price in page text...")

        # Look for price patterns in the entire page
        price_patterns = [
            r"(\d+(?:\s*\d+)*)\s*〒",  # Price with tenge symbol
            r"(\d+(?:\s*\d+)*)\s*тенге",  # Price with "тенге"
            r"(\d+(?:\s*\d+)*)\s*₸",  # Price with tenge symbol
            r"(\d+(?:\s*\d+)*)\s*тг",  # Price with "тг"
            r"(\d+(?:\s*\d+)*)\s*KZT",  # Price with KZT
        ]

        page_text = soup.get_text()
        for pattern in price_patterns:
            matches = re.findall(pattern, page_text, re.IGNORECASE)
            if matches:
                # Get the first reasonable price (not too small, not too large)
                for match in matches:
                    # Replace both regular spaces and non-breaking spaces
                    price_str = match.replace(" ", "").replace("\xa0", "")
                    if price_str:
                        price = int(price_str)
                        # Filter out unreasonable prices (too small or too large)
                        if 1000 <= price <= 10000000:  # Between 1K and 10M tenge
                            logging.info(f"Found price in text: {price}")
                            return price

        logging.warning("Could not extract price from any method")
        return None
    except Exception as e:
        logging.error(f"Error extracting price: {e}")
        return None


def extract_area(soup: BeautifulSoup) -> Optional[float]:
    """Extract area from the page."""
    try:
        # Look for area in various formats - updated selectors
        area_selectors = [
            '.offer__info-item:contains("Площадь")',
            '.offer__info-item:contains("м²")',
            '[data-testid="area"]',
            '.offer__info-item:contains("площадь")',
            '.offer__info-item:contains("кв.м")',
            '.offer__info-item:contains("кв м")',
            '.offer__info-item:contains("кв. м")',
            ".area",
            ".offer__area",
            ".offer__info-area",
        ]

        for selector in area_selectors:
            area_elem = soup.select_one(selector)
            if area_elem:
                area_text = area_elem.get_text(strip=True)
                logging.info(
                    f"Found area element with selector '{selector}': '{area_text}'"
                )

                # Extract numeric value
                area_match = re.search(r"(\d+(?:\.\d+)?)", area_text)
                if area_match:
                    area_value = float(area_match.group(1))
                    if 10 <= area_value <= 500:  # Reasonable area range
                        return area_value

        # If selectors don't work, try to find area in the page text
        logging.info("Trying to find area in page text...")

        # Look for area patterns in the entire page
        area_patterns = [
            r"(\d+(?:\.\d+)?)\s*м²",  # Area with м²
            r"(\d+(?:\.\d+)?)\s*кв\.м",  # Area with кв.м
            r"(\d+(?:\.\d+)?)\s*кв\s*м",  # Area with кв м
            r"(\d+(?:\.\d+)?)\s*кв\.\s*м",  # Area with кв. м
            r"(\d+(?:\.\d+)?)\s*квм",  # Area with квм
        ]

        page_text = soup.get_text()
        for pattern in area_patterns:
            matches = re.findall(pattern, page_text, re.IGNORECASE)
            if matches:
                # Get the first reasonable area
                for match in matches:
                    area_value = float(match)
                    if 10 <= area_value <= 500:  # Reasonable area range
                        logging.info(f"Found area in text: {area_value}")
                        return area_value

        logging.warning("Could not extract area from any method")
        return None
    except Exception as e:
        logging.error(f"Error extracting area: {e}")
        return None


def extract_residential_complex(soup: BeautifulSoup) -> Optional[str]:
    """Extract residential complex name."""
    try:
        complex_selectors = [
            '.offer__info-item:contains("ЖК")',
            '.offer__info-item:contains("Жилой комплекс")',
            '[data-testid="complex"]',
        ]

        for selector in complex_selectors:
            complex_elem = soup.select_one(selector)
            if complex_elem:
                complex_text = complex_elem.get_text(strip=True)

                # Remove common prefixes
                prefixes_to_remove = [
                    "Жилой комплекс",
                    "ЖК",
                    "Residential Complex",
                    "Complex",
                ]

                for prefix in prefixes_to_remove:
                    if complex_text.startswith(prefix):
                        complex_text = complex_text[len(prefix) :].strip()
                        break

                return complex_text if complex_text else None

        return None
    except Exception:
        return None


def extract_floor_info(soup: BeautifulSoup) -> tuple[Optional[int], Optional[int]]:
    """Extract floor and total floors information."""
    try:
        floor_selectors = [
            '.offer__info-item:contains("Этаж")',
            '.offer__info-item:contains("этаж")',
        ]

        for selector in floor_selectors:
            floor_elem = soup.select_one(selector)
            if floor_elem:
                floor_text = floor_elem.get_text(strip=True)
                # Look for patterns like "5 из 9" or "5/9"
                floor_match = re.search(r"(\d+)\s*(?:из|/)\s*(\d+)", floor_text)
                if floor_match:
                    return int(floor_match.group(1)), int(floor_match.group(2))
                # Look for single floor number
                single_floor_match = re.search(r"(\d+)", floor_text)
                if single_floor_match:
                    return int(single_floor_match.group(1)), None

        return None, None
    except Exception:
        return None, None


def extract_construction_year(soup: BeautifulSoup) -> Optional[int]:
    """Extract construction year."""
    try:
        year_selectors = [
            '.offer__info-item:contains("Год постройки")',
            '.offer__info-item:contains("год")',
        ]

        for selector in year_selectors:
            year_elem = soup.select_one(selector)
            if year_elem:
                year_text = year_elem.get_text(strip=True)
                year_match = re.search(r"(\d{4})", year_text)
                if year_match:
                    return int(year_match.group(1))

        return None
    except Exception:
        return None


def extract_parking_info(soup: BeautifulSoup) -> Optional[str]:
    """Extract parking information."""
    try:
        parking_selectors = [
            '.offer__info-item:contains("Парковка")',
            '.offer__info-item:contains("парковка")',
        ]

        for selector in parking_selectors:
            parking_elem = soup.select_one(selector)
            if parking_elem:
                return parking_elem.get_text(strip=True)

        return None
    except Exception:
        return None


def extract_description(soup: BeautifulSoup) -> str:
    """Extract flat description."""
    try:
        desc_selectors = [".offer__description", ".offer__text", ".description"]

        for selector in desc_selectors:
            desc_elem = soup.select_one(selector)
            if desc_elem:
                return desc_elem.get_text(strip=True)

        return "No description available"
    except Exception:
        return "No description available"


def determine_flat_type(area: float) -> str:
    """Determine flat type based on area."""
    if area <= 30:
        return "Studio"
    elif area <= 50:
        return "1BR"
    elif area <= 80:
        return "2BR"
    else:
        return "3BR+"


def determine_flat_type_from_text(
    title: str, description: str, area: Optional[float]
) -> str:
    """Derive flat type preferring title, then description, then area fallback."""
    combined = f"{title}\n{description}".lower()
    # Studio keywords
    if re.search(r"студи\w+", combined):
        return "Studio"
    # N-room patterns
    m = re.search(r"(\d+)\s*[-–]?\s*комнатн", combined)
    if m:
        try:
            rooms = int(m.group(1))
            if rooms <= 0:
                return determine_flat_type(area) if area is not None else "1BR"
            if rooms == 1:
                return "1BR"
            if rooms == 2:
                return "2BR"
            return "3BR+"
        except Exception:
            pass
    # Fallback to area-based determination
    if area is not None:
        return determine_flat_type(area)
    return "1BR"


def normalize_flat_type_enum(value: str) -> str:
    """Return canonical FlatType string value from free-text value."""
    v = (value or "").strip()
    if v.lower().startswith("studio") or re.search(r"студи\w+", v.lower()):
        return FlatType.STUDIO.value
    if v in {"1BR", "1Br", "1br"}:
        return FlatType.ONE_BEDROOM.value
    if v in {"2BR", "2Br", "2br"}:
        return FlatType.TWO_BEDROOM.value
    if v in {"3BR+", "3br+", "3BR", "4BR", "5BR"}:
        return FlatType.THREE_PLUS_BEDROOM.value
    # Fallback to normalization helper
    try:
        from common.src.flat_type import normalize_flat_type

        return normalize_flat_type(v)
    except Exception:
        return FlatType.ONE_BEDROOM.value


def extract_jk_from_description(description: str) -> Optional[str]:
    """Try to extract residential complex (JK) name from description text."""
    if not description:
        return None

    text = description.strip()

    # Patterns to match various JK notations
    # Include various dash types: regular hyphen (-), em-dash (—), en-dash (–), and minus (−)
    patterns = [
        r'жил\.?\s*комплекс\s+([A-Za-zА-Яа-яЁё0-9"“”\-\s–—−]+?)(?:[,\.|\n]|$)',
        r'ЖК\s*[""]([^""]+)[""]',
        r"ЖК\s+([A-Za-zА-Яа-яЁё0-9\-\s–—−]+?)(?:[,\.|\n]|$)",
    ]

    for pat in patterns:
        m = re.search(pat, text, flags=re.IGNORECASE)
        if m:
            name = m.group(1).strip()
            # Clean common trailing words
            name = re.sub(r"\s*(в|Алматы.*)$", "", name).strip()
            # Normalize fancy quotes
            name = name.replace('"', '"').replace('"', '"')
            # Normalize dash types (keep the original, but ensure consistency)
            # Don't replace dashes, just trim whitespace around them
            name = re.sub(
                r"\s+", " ", name
            )  # Normalize multiple spaces to single space
            # Remove surrounding quotes if any
            name = name.strip('"')
            # Basic length sanity
            if 2 <= len(name) <= 80:
                return name

    return None


def extract_additional_info_from_description(description: str) -> dict:
    """
    Extract additional optional fields from description text (JK, parking, construction year).
    :param description: str, raw description text
    :return: dict, possibly containing keys: residential_complex, parking, construction_year
    """
    if not description:
        return {}

    results: dict = {}
    text = description.strip()

    # Try JK via existing helper
    jk = extract_jk_from_description(text)
    if jk:
        results["residential_complex"] = jk

    # Construction year patterns
    year_patterns = [
        r"(?:год\s+постройки|построен[ао]?|сдан\s+в)\s*(\d{4})",
        r"\b(20\d{2}|19\d{2})\b\s*(?:г\.?|год[а]?\b)",
    ]
    for pat in year_patterns:
        m = re.search(pat, text, flags=re.IGNORECASE)
        if m:
            try:
                year = int(m.group(1))
                if 1900 <= year <= 2100:
                    results["construction_year"] = year
                    break
            except Exception:
                pass

    # Parking extraction
    parking_value = None
    if re.search(r"подземн\w*\s*парковк|паркинг", text, flags=re.IGNORECASE):
        parking_value = "подземная парковка"
    elif re.search(r"наземн\w*\s*парковк", text, flags=re.IGNORECASE):
        parking_value = "наземная парковка"
    elif re.search(r"охраняем\w*\s*стоянк", text, flags=re.IGNORECASE):
        parking_value = "охраняемая стоянка"
    elif re.search(r"парковк|паркинг", text, flags=re.IGNORECASE):
        parking_value = "парковка"

    if parking_value:
        results["parking"] = parking_value

    return results


def get_flat_urls_from_search_page(search_url: str) -> List[str]:
    """
    Extract flat URLs from a Krisha.kz search page.

    :param search_url: str, URL of the search page
    :return: List[str], list of flat URLs
    """
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            # Drop 'br' to avoid brotli when not available in tests/CI
            "Accept-Encoding": "gzip, deflate",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
        }

        response = fetch_url(search_url, headers=headers, timeout=10)

        # Prefer bytes to avoid decoding issues and rely on lxml/html5lib fallback if installed
        soup = BeautifulSoup(response.content, "html.parser")

        flat_urls: List[str] = []

        # Scope strictly to main results list to avoid right-hand ads
        list_container = soup.select_one(".a-list.a-search-list.a-list-with-favs")
        if list_container is None:
            list_container = soup.select_one(".a-list.a-search-list")

        def add_href(href: str) -> None:
            if not href or "/a/show/" not in href:
                return
            if href.startswith("/"):
                href_abs = f"https://krisha.kz{href}"
            elif not href.startswith("http"):
                href_abs = f"https://krisha.kz/{href}"
            else:
                href_abs = href
            if href_abs not in flat_urls:
                flat_urls.append(href_abs)

        if list_container is not None:
            # Anchors within the card titles inside the container
            for a in list_container.select('a.a-card__title[href*="/a/show/"]'):
                add_href(a.get("href"))
            # Conservative fallback within the same container only
            for a in list_container.select('a[href^="/a/show/"]'):
                add_href(a.get("href"))
        else:
            # As a last resort, search globally but still restrict to card title links
            for a in soup.select('a.a-card__title[href*="/a/show/"]'):
                add_href(a.get("href"))

        # Deduplicate preserving order
        seen = set()
        unique_urls: List[str] = []
        for u in flat_urls:
            if u not in seen:
                unique_urls.append(u)
                seen.add(u)

        logging.info(f"Found {len(unique_urls)} flat URLs on search page")
        return unique_urls

    except Exception as e:
        logging.error(f"Error extracting flat URLs from {search_url}: {e}")
        return []


def extract_flat_id_from_url(flat_url: str) -> Optional[str]:
    """
    Extract flat ID from a Krisha.kz flat URL.

    :param flat_url: str, URL like https://krisha.kz/a/show/123456789
    :return: Optional[str], flat ID or None if not found
    """
    try:
        # Extract ID from URL pattern
        match = re.search(r"/a/show/(\d+)", flat_url)
        if match:
            return match.group(1)
        return None
    except Exception as e:
        logging.error(f"Error extracting flat ID from {flat_url}: {e}")
        return None
