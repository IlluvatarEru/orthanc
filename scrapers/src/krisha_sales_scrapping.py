"""
Sales scrapping module for fetching individual sales flat information.

This module provides functionality to scrape sales flat details from Krisha.kz
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
from scrapers.src.utils import (
    extract_price, extract_area, extract_residential_complex,
    extract_floor_info, extract_construction_year, extract_parking_info,
    extract_description, determine_flat_type, determine_flat_type_from_text,
    normalize_flat_type_enum, extract_jk_from_description,
    extract_additional_info_from_description, get_flat_urls_from_search_page,
    extract_flat_id_from_url
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
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        }
        
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()
        
        # Parse HTML with BeautifulSoup
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Use the existing extract_sales_info function
        return extract_sales_info(soup, krisha_id, url)
        
    except requests.RequestException as e:
        logging.error(f"Request error scraping sales flat {krisha_id} from sale page: {e}")
        return None
    except Exception as e:
        logging.error(f"Unexpected error scraping sales flat {krisha_id} from sale page: {e}")
        return None


def scrape_sales_flat_from_analytics_page_with_failover_to_sale_page(krisha_id: str) -> Optional[FlatInfo]:
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
            logging.info(f"✅ Successfully scraped flat {krisha_id} using analytics API")
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
            logging.info(f"✅ Successfully scraped flat {krisha_id} using direct page scraping")
            return flat_info
        else:
            logging.warning(f"Direct page scraping returned None for flat {krisha_id}")
    except Exception as e:
        logging.warning(f"Direct page scraping failed for flat {krisha_id}: {e}")
    
    # Both methods failed
    logging.error(f"❌ Both scraping methods failed for flat {krisha_id}")
    return None


def scrape_sales_flat_from_analytics_page(krisha_id: str) -> Optional[FlatInfo]:
    """
    Fetch sales flat information via Krisha mobile analytics JSON and build FlatInfo.
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
        logging.info(response.text)

        try:
            data = response.json()
        except Exception:
            data = json.loads(response.text)

        advert = data.get('advert', {})

        # Price (total sales price) - extract from advert.price HTML
        price_html = advert.get('price', '')
        price_digits = re.sub(r'[^\d]', '', price_html)
        price = int(price_digits) if price_digits else None
        if price is None:
            logging.warning(f"No price found for sales flat {krisha_id}")
            return None

        # Area from title (e.g., "250 м²") or description
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
        if area is None:
            logging.warning(f"No area found in title for sales flat {krisha_id}")
            return None

        # Floors from title (e.g., "3/9 этаж")
        floor = None
        total_floors = None
        m_floor = re.search(r'(\d+)\s*/\s*(\d+)\s*этаж', title)
        if m_floor:
            floor = int(m_floor.group(1))
            total_floors = int(m_floor.group(2))

        # description already set above
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
            is_rental=False,  # Sales flat
        )

        return flat_info
    except requests.RequestException as e:
        logging.error(f"Request error scraping sales flat {krisha_id}: {e}")
        return None
    except Exception as e:
        logging.error(f"Unexpected error scraping sales flat {krisha_id}: {e}")
        return None

def extract_sales_info(soup: BeautifulSoup, flat_id: str, url: str) -> Optional[FlatInfo]:
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
            is_rental=False  # This is a sales flat
        )
        
        return flat_info
        
    except Exception as e:
        logging.error(f"Error extracting sales flat information: {e}")
        return None


def scrap_jk_sales(jk_name: str, max_pages: int = 10) -> List[FlatInfo]:
    """
    Scrape all sales flats for a specific residential complex (JK).
    
    :param jk_name: str, name of the residential complex
    :param max_pages: int, maximum number of pages to scrape (default: 10)
    :return: List[FlatInfo], list of scraped sales flats
    """
    logging.info(f"Starting JK sales scraping for: {jk_name}")
    logging.info(f"Max pages: {max_pages}")
    
    all_flats = []
    
    # Search for the complex to get its ID
    from scrapers.src.residential_complex_scraper import search_complex_by_name
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
        search_url = f"https://krisha.kz/prodazha/kvartiry/almaty/?das[map.complex]={complex_id}&page={page}"
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
                
                # Scrape the flat with failover
                flat_info = scrape_sales_flat_from_analytics_page_with_failover_to_sale_page(flat_id)
                if flat_info:
                    all_flats.append(flat_info)
                    logging.info(f"Successfully scraped sales flat: {flat_id}")
                else:
                    logging.error(f"Failed to scrape sales flat: {flat_id}")
                    
            except Exception as e:
                logging.error(f"Error scraping flat from {flat_url}: {e}")
                continue
    
    logging.info(f"Completed JK sales scraping for {jk_name}. Total flats: {len(all_flats)}")
    return all_flats


def scrap_and_save_jk_sales(jk_name: str, max_pages: int = 10, db_path: str = "flats.db") -> int:
    """
    Scrape and save all sales flats for a specific residential complex (JK).
    
    :param jk_name: str, name of the residential complex
    :param max_pages: int, maximum number of pages to scrape (default: 10)
    :param db_path: str, path to database file
    :return: int, number of flats saved to database
    """
    logging.info(f"Starting JK sales scraping and saving for: {jk_name}")
    
        # Scrape flats
    flats = scrap_jk_sales(jk_name, max_pages)

    if not flats:
        logging.warning(f"No flats found for {jk_name}")
        return 0

    # Save to database
    from db.src.write_read_database import OrthancDB
    logging.info(f"Initializing database connection to: {db_path}")
    db = OrthancDB(db_path)
    saved_count = 0
        
    for flat_info in flats:
        try:
            # Save sales flat to database
            success = db.insert_sales_flat(
                flat_info=flat_info,
                url=f"https://krisha.kz/a/show/{flat_info.flat_id}",
                query_date=datetime.now().strftime('%Y-%m-%d'),
                flat_type=flat_info.flat_type
            )

            if success:
                saved_count += 1
                logging.info(f"Saved sales flat: {flat_info.flat_id}")
            else:
                logging.error(f"Failed to save sales flat: {flat_info.flat_id}")

        except Exception as e:
            logging.error(f"Error saving flat {flat_info.flat_id}: {e}")
            continue
        
        logging.info(f"Completed JK sales scraping and saving for {jk_name}. Saved: {saved_count}/{len(flats)}")
        return saved_count


