"""
Krisha.kz web scraper for extracting flat information.

This module provides functionality to scrape flat details from Krisha.kz
using their API endpoint.
"""
import re
import json

import requests

import logging
from common.src.flat_info import FlatInfo
from scrapers.src.utils import extract_flat_id_from_url, extract_area_from_title, extract_floor_info_from_title, \
    extract_residential_complex_from_description, extract_construction_year_from_description, \
    extract_parking_info_from_description


def detect_rental_from_api_data(data: dict) -> bool:
    """
    Detect if a flat is for rent based on API response data.
    
    :param data: dict, API response data
    :return: bool, True if rental, False if sale
    """
    # Check multiple indicators of rental vs sale
    advert = data.get('advert', {})
    
    # 1. PRIORITY: Check if price is monthly (rental) vs total (sale)
    # This is the most reliable indicator
    price_text = advert.get('price', '')
    if price_text:
        # Clean price text the same way as in scrape_flat_info
        price_text_clean = price_text.replace('&nbsp;', ' ').replace('<span class="currency-sign offer__currency">₸</span>', '').replace('<span class="currency-sign offer__currency">〒</span>', '')
        # Extract numeric price
        price_match = re.search(r'(\d+(?:\s*\d+)*)', price_text_clean)
        if price_match:
            price = int(price_match.group(1).replace(' ', ''))
            # If price is less than 1 million tenge, likely rental
            # Sale prices are typically 10M+ tenge
            if price < 1000000:
                return True
            elif price > 5000000:
                return False
    
    # 2. Check the title for rental keywords
    title = advert.get('title', '').lower()
    rental_keywords = ['аренда', 'сдам', 'сдаю', 'rent', 'rental', 'аренду', 'сдаётся']
    sale_keywords = ['продажа', 'продам', 'продаю', 'sale', 'buy', 'куплю', 'продаётся']
    
    for keyword in rental_keywords:
        if keyword in title:
            return True
    
    for keyword in sale_keywords:
        if keyword in title:
            return False
    
    # 3. Check the description for rental keywords (exact word matches)
    description = advert.get('description', '').lower()
    description_words = description.split()
    for keyword in rental_keywords:
        if keyword in description_words:
            return True
    
    for keyword in sale_keywords:
        if keyword in description_words:
            return False
    
    # 4. Check URL pattern (if available in data)
    url = advert.get('url', '')
    if 'arenda' in url.lower():
        return True
    elif 'prodazha' in url.lower():
        return False
    
    # 5. Check for specific rental indicators in description (exact word matches)
    rental_indicators = ['месяц', 'месячная', 'ежемесячно', 'арендная плата']
    for indicator in rental_indicators:
        if indicator in description_words:
            return True
    
    # Default to sale if we can't determine
    return False


def scrape_flat_info(url: str) -> FlatInfo:
    """
    Scrape flat information from Krisha.kz using their API.
    
    :param url: str, URL of the flat page (e.g., https://krisha.kz/a/show/1003924251)
    :return: FlatInfo, extracted flat information
    """
    # Extract flat ID from URL
    flat_id = extract_flat_id_from_url(url)
    
    # API endpoint
    api_url = f"https://m.krisha.kz/analytics/aPriceAnalysis/?id={flat_id}"
    
    # Headers to mimic browser request
    headers = {
        'User-Agent': 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Mobile Safari/537.36',
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
        'Content-Type': 'application/x-www-form-urlencoded',
        'Origin': 'https://m.krisha.kz',
        'Referer': f'https://m.krisha.kz/a/show/{flat_id}',
        'X-Requested-With': 'XMLHttpRequest'
    }
    
    # Make POST request to API
    response = requests.post(api_url, headers=headers, data=f"id={flat_id}")
    
    if response.status_code != 200:
        raise Exception(f"Failed to fetch data: {response.status_code}")
    
    # Check if response has content
    if not response.text.strip():
        raise Exception(f"Empty response from API for flat {flat_id}")
    
    # Parse JSON response with error handling
    logging.info(f"response for {api_url} =\n{response.text}")
    try:
        data = response.json()
    except json.JSONDecodeError as e:
        logging.error(f"Failed to parse JSON response for flat {flat_id}: {e}")
        logging.error(f"Response content: {response.text[:500]}")  # Log first 500 chars
        raise Exception(f"Invalid JSON response from API for flat {flat_id}: {e}")
    
    # Validate response structure
    if not isinstance(data, dict):
        raise Exception(f"API returned invalid data structure for flat {flat_id}")
    
    # Extract advert information
    advert = data.get('advert', {})
    
    # Get text - it's already in Unicode format
    title = advert.get('title', '')
    description = advert.get('description', '')
    
    # Extract price (remove HTML tags and convert to integer)
    price_text = advert.get('price', '')
    # Remove HTML tags and non-breaking spaces
    price_text_clean = price_text.replace('&nbsp;', ' ').replace('<span class="currency-sign offer__currency">₸</span>', '').replace('<span class="currency-sign offer__currency">〒</span>', '')
    price_match = re.search(r'(\d+(?:\s*\d+)*)', price_text_clean)
    price = int(price_match.group(1).replace(' ', '')) if price_match else 0
    
    # Extract area from title
    area = extract_area_from_title(title)
    
    # Extract floor information from title
    floor, total_floors = extract_floor_info_from_title(title)
    
    # Extract residential complex from description
    residential_complex = extract_residential_complex_from_description(description)
    
    # Extract construction year from description
    construction_year = extract_construction_year_from_description(description)
    
    # Extract parking information from description
    parking = extract_parking_info_from_description(description)
    
    # Detect if the flat is for rent
    is_rental = detect_rental_from_api_data(data)
    
    return FlatInfo(
        flat_id=flat_id,
        price=price,
        area=area or 0.0,
        residential_complex=residential_complex,
        floor=floor,
        total_floors=total_floors,
        construction_year=construction_year,
        parking=parking,
        description=description,
        is_rental=is_rental
    )


def main():
    """
    Example usage of the scraper.
    """
    # Example URL
    url = "https://krisha.kz/a/show/1003924251"
    
    try:
        flat_info = scrape_flat_info(url)
        logging.info("Extracted flat information:")
        logging.info(f"Flat ID: {flat_info.flat_id}")
        logging.info(f"Price: {flat_info.price:,} tenge")
        logging.info(f"Area: {flat_info.area} m²")
        logging.info(f"Residential Complex: {flat_info.residential_complex}")
        logging.info(f"Floor: {flat_info.floor}/{flat_info.total_floors}")
        logging.info(f"Construction Year: {flat_info.construction_year}")
        logging.info(f"Parking: {flat_info.parking}")
        logging.info(f"Description: {flat_info.description[:200]}...")
        logging.info(f"Is Rental: {flat_info.is_rental}")
        
    except Exception as e:
        logging.info(f"Error scraping flat info: {e}")


def determine_flat_type(area: float, description: str) -> str:
    """
    Determine flat type based on area and description.
    
    :param area: float, area in square meters
    :param description: str, flat description
    :return: str, flat type ('Studio', '1BR', '2BR', '3BR+')
    """
    description_lower = description.lower()
    
    # Check for explicit mentions in description
    if any(word in description_lower for word in ['студия', 'studio', 'студио']):
        return 'Studio'
    elif any(word in description_lower for word in ['1-комнатная', '1-комн', '1 комната', '1 комн', 'однокомнатная', 'однокомн']):
        return '1BR'
    elif any(word in description_lower for word in ['2-комнатная', '2-комн', '2 комнаты', '2 комн', 'двухкомнатная', 'двухкомн']):
        return '2BR'
    elif any(word in description_lower for word in ['3-комнатная', '3-комн', '3 комнаты', '3 комн', 'трехкомнатная', 'трехкомн', '4-комнатная', '4-комн', '4 комнаты', '4 комн', 'четырехкомнатная', 'четырехкомн']):
        return '3BR+'
    
    # Fallback to area-based classification
    if area <= 35:
        return 'Studio'
    elif area <= 50:
        return '1BR'
    elif area <= 80:
        return '2BR'
    else:
        return '3BR+'


def scrape_flat_info_with_type(url: str) -> FlatInfo:
    """
    Scrape flat information and determine flat type.
    
    :param url: str, URL of the flat to scrape
    :return: FlatInfo, flat information with determined type
    """
    flat_info = scrape_flat_info(url)
    if flat_info:
        flat_info.flat_type = determine_flat_type(flat_info.area, flat_info.description)
    return flat_info


if __name__ == "__main__":
    main()



