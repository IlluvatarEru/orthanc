"""
Krisha.kz web scraper for extracting flat information.

This module provides functionality to scrape flat details from Krisha.kz
using their API endpoint.
"""

import re
import requests
import json
from dataclasses import dataclass
from typing import Optional
from urllib.parse import urlparse


@dataclass
class FlatInfo:
    """
    Data class to store flat information extracted from Krisha.kz.
    
    :param flat_id: str, unique identifier of the flat from URL
    :param price: int, price in tenge
    :param area: float, area in square meters
    :param residential_complex: Optional[str], name of the residential complex (JK)
    :param floor: Optional[int], floor number where the flat is located
    :param total_floors: Optional[int], total number of floors in the building
    :param construction_year: Optional[int], year of construction
    :param parking: Optional[str], parking information
    :param description: str, full description text
    """
    flat_id: str
    price: int
    area: float
    residential_complex: Optional[str]
    floor: Optional[int]
    total_floors: Optional[int]
    construction_year: Optional[int]
    parking: Optional[str]
    description: str


def extract_flat_id_from_url(url: str) -> str:
    """
    Extract flat ID from Krisha.kz URL.
    
    :param url: str, URL like https://krisha.kz/a/show/1003924251
    :return: str, flat ID extracted from URL
    """
    # Extract ID from URL pattern like /a/show/1003924251
    match = re.search(r'/a/show/(\d+)', url)
    if not match:
        raise ValueError(f"Could not extract flat ID from URL: {url}")
    return match.group(1)


def decode_cyrillic_text(text: str) -> str:
    """
    Decode Unicode escape sequences for Cyrillic text.
    
    :param text: str, text with Unicode escape sequences
    :return: str, decoded Cyrillic text
    """
    try:
        # First try to decode as unicode escape sequences
        return text.encode('utf-8').decode('unicode_escape')
    except (UnicodeDecodeError, UnicodeEncodeError):
        # If that fails, try direct decoding
        return text


def extract_area_from_title(title: str) -> Optional[float]:
    """
    Extract area from title text.
    
    :param title: str, title containing area information
    :return: Optional[float], area in square meters
    """
    # Look for pattern like "33 м²" or "33 м2" or "33 м"
    # The м² character is Unicode U+00B2 (superscript 2)
    match = re.search(r'(\d+(?:\.\d+)?)\s*м[²2]?', title)
    if match:
        return float(match.group(1))
    
    # Also try to match the Unicode superscript 2 character
    match = re.search(r'(\d+(?:\.\d+)?)\s*м\u00B2', title)
    if match:
        return float(match.group(1))
    
    return None


def extract_floor_info_from_title(title: str) -> tuple[Optional[int], Optional[int]]:
    """
    Extract floor and total floors from title text.
    
    :param title: str, title containing floor information
    :return: tuple[Optional[int], Optional[int]], (floor, total_floors)
    """
    # Look for pattern like "10/12 этаж" or "10/12"
    match = re.search(r'(\d+)/(\d+)\s*этаж', title)
    if match:
        return int(match.group(1)), int(match.group(2))
    
    # Also try to find just the pattern "10/12" without "этаж"
    match = re.search(r'(\d+)/(\d+)', title)
    if match:
        return int(match.group(1)), int(match.group(2))
    
    return None, None


def extract_residential_complex_from_description(description: str) -> Optional[str]:
    """
    Extract residential complex name from description.
    
    :param description: str, full description text
    :return: Optional[str], residential complex name
    """
    # Look for patterns like "жил. комплекс", "ЖК", etc.
    patterns = [
        r'жил\.\s*комплекс\s+([^,\.]+)',
        r'ЖК\s+([^,\.]+)',
        r'жилой\s+комплекс\s+([^,\.]+)'
    ]
    
    for pattern in patterns:
        match = re.search(pattern, description, re.IGNORECASE)
        if match:
            return match.group(1).strip()
    return None


def extract_construction_year_from_description(description: str) -> Optional[int]:
    """
    Extract construction year from description.
    
    :param description: str, full description text
    :return: Optional[int], construction year
    """
    # Look for year patterns like "2024 г.п." or "2024 г"
    match = re.search(r'(\d{4})\s*г\.?п?\.?', description)
    if match:
        return int(match.group(1))
    return None


def extract_parking_info_from_description(description: str) -> Optional[str]:
    """
    Extract parking information from description.
    
    :param description: str, full description text
    :return: Optional[str], parking information
    """
    # Look for parking-related keywords
    parking_keywords = ['парковка', 'паркинг', 'подземная парковка', 'наземная парковка']
    
    for keyword in parking_keywords:
        if keyword.lower() in description.lower():
            return keyword
    return None


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
    
    # Parse JSON response
    data = response.json()
    
    # Extract advert information
    advert = data.get('advert', {})
    
    # Get text - it's already in Unicode format
    title = advert.get('title', '')
    description = advert.get('description', '')
    
    # Extract price (remove HTML tags and convert to integer)
    price_text = advert.get('price', '')
    # Remove HTML tags and non-breaking spaces
    price_text_clean = price_text.replace('&nbsp;', ' ').replace('<span class="currency-sign offer__currency">₸</span>', '')
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
    
    return FlatInfo(
        flat_id=flat_id,
        price=price,
        area=area or 0.0,
        residential_complex=residential_complex,
        floor=floor,
        total_floors=total_floors,
        construction_year=construction_year,
        parking=parking,
        description=description
    )


def main():
    """
    Example usage of the scraper.
    """
    # Example URL
    url = "https://krisha.kz/a/show/1003924251"
    
    try:
        flat_info = scrape_flat_info(url)
        print("Extracted flat information:")
        print(f"Flat ID: {flat_info.flat_id}")
        print(f"Price: {flat_info.price:,} tenge")
        print(f"Area: {flat_info.area} m²")
        print(f"Residential Complex: {flat_info.residential_complex}")
        print(f"Floor: {flat_info.floor}/{flat_info.total_floors}")
        print(f"Construction Year: {flat_info.construction_year}")
        print(f"Parking: {flat_info.parking}")
        print(f"Description: {flat_info.description[:200]}...")
        
    except Exception as e:
        print(f"Error scraping flat info: {e}")


if __name__ == "__main__":
    main() 