"""
Rental scrapping module for fetching individual rental flat information.

This module provides functionality to scrape rental flat details from Krisha.kz
using a single flat ID and return a FlatInfo object.
"""

import requests
import re
import json
import logging
from typing import Optional, List
from datetime import datetime
from bs4 import BeautifulSoup
from common.src.flat_info import FlatInfo
from common.src.flat_type import FlatType


def scrape_rental_flat(krisha_id: str) -> Optional[FlatInfo]:
    """
    Fetch rental flat information via Krisha mobile analytics JSON and build FlatInfo.
    :param krisha_id: str, Krisha.kz flat ID (e.g., "12345678")
    :return: Optional[FlatInfo], flat information object or None if failed
    """
    try:
        api_url = f"https://m.krisha.kz/analytics/aPriceAnalysis/?id={krisha_id}"

        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
            'Accept': 'application/json, text/plain, */*',
            'Referer': f'https://m.krisha.kz/a/show/{krisha_id}',
            'Origin': 'https://m.krisha.kz',
        }

        response = requests.get(api_url, headers=headers, timeout=15)
        response.raise_for_status()

        try:
            data = response.json()
        except Exception:
            data = json.loads(response.text)

        advert = data.get('advert', {})

        # Price (monthly rent)
        price = data.get('currentPrice')
        if price is None:
            price_html = advert.get('price', '')
            price_digits = re.sub(r'[^\d]', '', price_html)
            price = int(price_digits) if price_digits else None
        if price is None:
            raise Exception(f"No price found for rental flat {krisha_id} at: {api_url}")


        # Area from title (e.g., "250 м²") or description if not in title
        title = advert.get('title', '')
        description = advert.get('description', '') or 'No description available'
        area = None
        m_area = re.search(r'(\d+(?:[\.,]\d+)?)\s*м²', title)
        if m_area:
            area = float(m_area.group(1).replace(',', '.'))
        else:
            m_area = re.search(r'(\d+(?:[\.,]\d+)?)\s*м²', description)
            if m_area:
                area = float(m_area.group(1).replace(',', '.'))

        # Floors from title (e.g., "3/9 этаж")
        floor = None
        total_floors = None
        m_floor = re.search(r'(\d+)\s*/\s*(\d+)\s*этаж', title)
        if m_floor:
            floor = int(m_floor.group(1))
            total_floors = int(m_floor.group(2))
        else:
            m_floor = re.search(r'(\d+)\s*/\s*(\d+)\s*этаж', description)
            if m_floor:
                floor = int(m_floor.group(1))
                total_floors = int(m_floor.group(2))

        # Extract JK and other fields from description
        residential_complex = extract_jk_from_description(description)
        extra = extract_additional_info_from_description(description)
        if not residential_complex:
            residential_complex = extra.get('residential_complex')

        flat_type = determine_flat_type_from_text(title, description, area)

        flat_info = FlatInfo(
            flat_id=str(krisha_id),
            price=int(price),
            area=area,
            flat_type=normalize_flat_type_enum(flat_type),
            residential_complex=residential_complex,
            floor=floor,
            total_floors=total_floors,
            construction_year=extra.get('construction_year'),
            parking=extra.get('parking'),
            description=description,
            is_rental=True,
        )

        logging.info(f"Successfully scraped rental flat {krisha_id} via analytics API")
        return flat_info
            
    except requests.RequestException as e:
        logging.error(f"Request error scraping rental flat {krisha_id}: {e}")
        return None
    except Exception as e:
        logging.error(f"Unexpected error scraping rental flat {krisha_id}: {e}")
        return None


def extract_rental_info(soup: BeautifulSoup, flat_id: str, url: str) -> Optional[FlatInfo]:
    """
    Extract rental flat information from parsed HTML.
    
    :param soup: BeautifulSoup, parsed HTML content
    :param flat_id: str, flat ID
    :param url: str, original URL
    :return: Optional[FlatInfo], extracted flat information
    """
    try:
        # Extract price (monthly rent)
        price = extract_price(soup)
        if not price:
            logging.warning(f"Could not extract price for rental flat {flat_id}")
            return None
        
        # Extract area
        area = extract_area(soup)
        if not area:
            logging.warning(f"Could not extract area for rental flat {flat_id}")
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
        
        # Determine flat type based on area
        flat_type = determine_flat_type(area)
        
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
            is_rental=True
        )
        
        return flat_info
        
    except Exception as e:
        logging.error(f"Error extracting rental flat information: {e}")
        return None


def extract_price(soup: BeautifulSoup) -> Optional[int]:
    """Extract price from the page."""
    try:
        # Look for price in various formats - updated selectors for current Krisha.kz structure
        price_selectors = [
            '.offer__price',
            '.price',
            '[data-testid="price"]',
            '.offer__sidebar .offer__price',
            '.offer__price-value',
            '.price-value',
            '.offer__price-value',
            '.offer__price .price-value',
            '.offer__price-value .price-value',
            # Look for price in title or main content
            'h1 .offer__price',
            '.offer__title .offer__price',
            # Look for price patterns in text
            '.offer__price-text',
            '.price-text'
        ]
        
        for selector in price_selectors:
            price_elem = soup.select_one(selector)
            if price_elem:
                price_text = price_elem.get_text(strip=True)
                logging.info(f"Found price element with selector '{selector}': '{price_text}'")
                
                # Extract numeric value - handle various formats
                # Remove common currency symbols and text
                clean_text = re.sub(r'[^\d\s]', '', price_text)
                price_match = re.search(r'(\d+(?:\s*\d+)*)', clean_text)
                if price_match:
                    price_str = price_match.group(1).replace(' ', '')
                    if price_str:
                        return int(price_str)
        
        # If selectors don't work, try to find price in the page text
        logging.info("Trying to find price in page text...")
        
        # Look for price patterns in the entire page
        price_patterns = [
            r'(\d+(?:\s*\d+)*)\s*〒',  # Price with tenge symbol
            r'(\d+(?:\s*\d+)*)\s*тенге',  # Price with "тенге"
            r'(\d+(?:\s*\d+)*)\s*₸',  # Price with tenge symbol
            r'(\d+(?:\s*\d+)*)\s*тг',  # Price with "тг"
            r'(\d+(?:\s*\d+)*)\s*KZT',  # Price with KZT
        ]
        
        page_text = soup.get_text()
        for pattern in price_patterns:
            matches = re.findall(pattern, page_text, re.IGNORECASE)
            if matches:
                # Get the first reasonable price (not too small, not too large)
                for match in matches:
                    price_str = match.replace(' ', '')
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
            '.area',
            '.offer__area',
            '.offer__info-area'
        ]
        
        for selector in area_selectors:
            area_elem = soup.select_one(selector)
            if area_elem:
                area_text = area_elem.get_text(strip=True)
                logging.info(f"Found area element with selector '{selector}': '{area_text}'")
                
                # Extract numeric value
                area_match = re.search(r'(\d+(?:\.\d+)?)', area_text)
                if area_match:
                    area_value = float(area_match.group(1))
                    if 10 <= area_value <= 500:  # Reasonable area range
                        return area_value
        
        # If selectors don't work, try to find area in the page text
        logging.info("Trying to find area in page text...")
        
        # Look for area patterns in the entire page
        area_patterns = [
            r'(\d+(?:\.\d+)?)\s*м²',  # Area with м²
            r'(\d+(?:\.\d+)?)\s*кв\.м',  # Area with кв.м
            r'(\d+(?:\.\d+)?)\s*кв\s*м',  # Area with кв м
            r'(\d+(?:\.\d+)?)\s*кв\.\s*м',  # Area with кв. м
            r'(\d+(?:\.\d+)?)\s*квм',  # Area with квм
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
            '[data-testid="complex"]'
        ]
        
        for selector in complex_selectors:
            complex_elem = soup.select_one(selector)
            if complex_elem:
                return complex_elem.get_text(strip=True)
        
        return None
    except Exception:
        return None


def extract_floor_info(soup: BeautifulSoup) -> tuple[Optional[int], Optional[int]]:
    """Extract floor and total floors information."""
    try:
        floor_selectors = [
            '.offer__info-item:contains("Этаж")',
            '.offer__info-item:contains("этаж")'
        ]
        
        for selector in floor_selectors:
            floor_elem = soup.select_one(selector)
            if floor_elem:
                floor_text = floor_elem.get_text(strip=True)
                # Look for patterns like "5 из 9" or "5/9"
                floor_match = re.search(r'(\d+)\s*(?:из|/)\s*(\d+)', floor_text)
                if floor_match:
                    return int(floor_match.group(1)), int(floor_match.group(2))
                # Look for single floor number
                single_floor_match = re.search(r'(\d+)', floor_text)
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
            '.offer__info-item:contains("год")'
        ]
        
        for selector in year_selectors:
            year_elem = soup.select_one(selector)
            if year_elem:
                year_text = year_elem.get_text(strip=True)
                year_match = re.search(r'(\d{4})', year_text)
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
            '.offer__info-item:contains("парковка")'
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
        desc_selectors = [
            '.offer__description',
            '.offer__text',
            '.description'
        ]
        
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


def determine_flat_type_from_text(title: str, description: str, area: Optional[float]) -> str:
    """Derive flat type preferring title, then description, then area fallback."""
    combined = f"{title}\n{description}".lower()
    # Studio keywords
    if re.search(r'студи\w+', combined):
        return "Studio"
    # N-room patterns
    m = re.search(r'(\d+)\s*[-–]?\s*комнатн', combined)
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
    if v.lower().startswith("studio") or re.search(r'студи\w+', v.lower()):
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
    patterns = [
        r'жил\.?\s*комплекс\s+([A-Za-zА-Яа-яЁё0-9"“”\-\s]+?)(?:[,\.|\n]|$)',
        r'ЖК\s*["“]([^"”]+)["”]',
        r'ЖК\s+([A-Za-zА-Яа-яЁё0-9\-\s]+?)(?:[,\.|\n]|$)'
    ]

    for pat in patterns:
        m = re.search(pat, text, flags=re.IGNORECASE)
        if m:
            name = m.group(1).strip()
            # Clean common trailing words
            name = re.sub(r'\s*(в|Алматы.*)$', '', name).strip()
            # Normalize fancy quotes
            name = name.replace('“', '"').replace('”', '"')
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
        results['residential_complex'] = jk

    # Construction year patterns
    year_patterns = [
        r'(?:год\s+постройки|построен[ао]?|сдан\s+в)\s*(\d{4})',
        r'\b(20\d{2}|19\d{2})\b\s*(?:г\.?|год[а]?\b)'
    ]
    for pat in year_patterns:
        m = re.search(pat, text, flags=re.IGNORECASE)
        if m:
            try:
                year = int(m.group(1))
                if 1900 <= year <= 2100:
                    results['construction_year'] = year
                    break
            except Exception:
                pass

    # Parking extraction
    parking_value = None
    if re.search(r'подземн\w*\s*парковк|паркинг', text, flags=re.IGNORECASE):
        parking_value = 'подземная парковка'
    elif re.search(r'наземн\w*\s*парковк', text, flags=re.IGNORECASE):
        parking_value = 'наземная парковка'
    elif re.search(r'охраняем\w*\s*стоянк', text, flags=re.IGNORECASE):
        parking_value = 'охраняемая стоянка'
    elif re.search(r'парковк|паркинг', text, flags=re.IGNORECASE):
        parking_value = 'парковка'

    if parking_value:
        results['parking'] = parking_value

    return results


def scrap_jk_rentals(jk_name: str, max_pages: int = 10) -> List[FlatInfo]:
    """
    Scrape all rental flats for a specific residential complex (JK).
    
    :param jk_name: str, name of the residential complex
    :param max_pages: int, maximum number of pages to scrape (default: 10)
    :return: List[FlatInfo], list of scraped rental flats
    """
    logging.info(f"Starting JK rental scraping for: {jk_name}")
    logging.info(f"Max pages: {max_pages}")
    
    all_flats = []
    
    # Search for the complex to get its ID
    from scrapers.src.complex_scraper import search_complex_by_name
    complex_info = search_complex_by_name(jk_name)

    if not complex_info or not complex_info.get('complex_id'):
        logging.error(f"Could not find complex ID for: {jk_name}")
        return []

    complex_id = complex_info['complex_id']
    logging.info(f"Found complex ID: {complex_id}")

    # Scrape each page
    for page in range(1, max_pages + 1):
        logging.info(f"Scraping page {page} for {jk_name}")

        # Construct search URL for this page
        search_url = f"https://krisha.kz/arenda/kvartiry/almaty/?das[map.complex]={complex_id}&page={page}"
        logging.info(search_url)

        # Get flat URLs from this page
        flat_urls = get_flat_urls_from_search_page(search_url)

        if not flat_urls:
            logging.info(f"No flats found on page {page}, stopping pagination")
            break

        logging.info(f"Found {len(flat_urls)} flats on page {page}")

        # Scrape each flat
        for flat_url in flat_urls:
            try:
                # Extract flat ID from URL
                flat_id = extract_flat_id_from_url(flat_url)
                if not flat_id:
                    continue

                # Scrape the flat
                flat_info = scrape_rental_flat(flat_id)
                if flat_info:
                    all_flats.append(flat_info)
                    logging.info(f"Successfully scraped rental flat: {flat_id}")
                else:
                    logging.warning(f"Failed to scrape rental flat: {flat_id}")

            except Exception as e:
                logging.error(f"Error scraping flat from {flat_url}: {e}")
                continue

    logging.info(f"Completed JK rental scraping for {jk_name}. Total flats: {len(all_flats)}")
    return all_flats



def scrap_and_save_jk_rentals(jk_name: str, max_pages: int = 10, db_path: str = "flats.db") -> int:
    """
    Scrape and save all rental flats for a specific residential complex (JK).
    
    :param jk_name: str, name of the residential complex
    :param max_pages: int, maximum number of pages to scrape (default: 10)
    :param db_path: str, path to database file
    :return: int, number of flats saved to database
    """
    logging.info(f"Starting JK rental scraping and saving for: {jk_name}")
    
    try:
        # Scrape flats
        flats = scrap_jk_rentals(jk_name, max_pages)
        
        if not flats:
            logging.warning(f"No flats found for {jk_name}")
            return 0
        
        # Save to database
        from db.src.write_read_database import OrthancDB
        db = OrthancDB(db_path)
        saved_count = 0
        
        try:
            db.connect()
            
            for flat_info in flats:
                try:
                    # Save rental flat to database
                    success = db.insert_rental_flat(
                        flat_info=flat_info,
                        url=f"https://krisha.kz/a/show/{flat_info.flat_id}",
                        query_date=datetime.now().strftime('%Y-%m-%d'),
                        flat_type=flat_info.flat_type
                    )
                    
                    if success:
                        saved_count += 1
                        logging.info(f"Saved rental flat: {flat_info.flat_id}")
                    else:
                        logging.warning(f"Failed to save rental flat: {flat_info.flat_id}")
                        
                except Exception as e:
                    logging.error(f"Error saving flat {flat_info.flat_id}: {e}")
                    continue
            
            db.conn.commit()
            
        finally:
            db.disconnect()
        
        logging.info(f"Completed JK rental scraping and saving for {jk_name}. Saved: {saved_count}/{len(flats)}")
        return saved_count
        
    except Exception as e:
        logging.error(f"Error during JK rental scraping and saving for {jk_name}: {e}")
        return 0


def get_flat_urls_from_search_page(search_url: str) -> List[str]:
    """
    Extract flat URLs from a Krisha.kz search page.
    
    :param search_url: str, URL of the search page
    :return: List[str], list of flat URLs
    """
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            # Drop 'br' to avoid brotli when not available in tests/CI
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        }

        response = requests.get(search_url, headers=headers, timeout=10)
        response.raise_for_status()

        # Prefer bytes to avoid decoding issues and rely on lxml/html5lib fallback if installed
        soup = BeautifulSoup(response.content, 'html.parser')

        flat_urls: List[str] = []

        # Scope strictly to main results list to avoid right-hand ads
        list_container = soup.select_one('.a-list.a-search-list.a-list-with-favs')
        if list_container is None:
            list_container = soup.select_one('.a-list.a-search-list')

        def add_href(href: str) -> None:
            if not href or '/a/show/' not in href:
                return
            if href.startswith('/'):
                href_abs = f"https://krisha.kz{href}"
            elif not href.startswith('http'):
                href_abs = f"https://krisha.kz/{href}"
            else:
                href_abs = href
            if href_abs not in flat_urls:
                flat_urls.append(href_abs)

        if list_container is not None:
            # Anchors within the card titles inside the container
            for a in list_container.select('a.a-card__title[href*="/a/show/"]'):
                add_href(a.get('href'))
            # Conservative fallback within the same container only
            for a in list_container.select('a[href^="/a/show/"]'):
                add_href(a.get('href'))
        else:
            # As a last resort, search globally but still restrict to card title links
            for a in soup.select('a.a-card__title[href*="/a/show/"]'):
                add_href(a.get('href'))

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
        match = re.search(r'/a/show/(\d+)', flat_url)
        if match:
            return match.group(1)
        return None
    except Exception as e:
        logging.error(f"Error extracting flat ID from {flat_url}: {e}")
        return None
