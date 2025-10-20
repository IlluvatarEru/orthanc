import re
from typing import Optional


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
    parking_keywords = ['парковка', 'парковочное место', 'гараж', 'parking']
    description_lower = description.lower()

    for keyword in parking_keywords:
        if keyword in description_lower:
            # Extract the sentence containing parking info
            sentences = description.split('.')
            for sentence in sentences:
                if keyword in sentence.lower():
                    return sentence.strip()

    return None
