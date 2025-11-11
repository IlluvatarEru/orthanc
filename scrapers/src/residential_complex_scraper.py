"""
Residential Complex Scraper for Krisha.kz

This module fetches residential complex data from Krisha.kz API
and stores it in the database for mapping complex IDs to names.
"""

from typing import List, Dict, Optional

import requests
import time

from db.src.write_read_database import OrthancDB
from scrapers.src.utils import get_flat_urls_from_search_page, extract_flat_id_from_url
import logging


def fetch_residential_complexes() -> List[Dict]:
    """
    Fetch all residential complexes from Krisha.kz API.

    :return: List[Dict], list of residential complexes
    """
    logging.info("Fetching residential complexes from Krisha.kz...")

    # API endpoint for residential complexes
    url = "https://krisha.kz/complex/ajaxMapComplexGetAll"

    # Headers to mimic browser (removed accept-encoding to prevent compression issues)
    headers = {
        "accept": "application/json, text/plain, */*",
        "accept-language": "en-US,en;q=0.9",
        "referer": "https://krisha.kz/complex/search/almaty/",
        "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36",
        "x-requested-with": "XMLHttpRequest",
    }

    # Cookies for region, no impact it seems
    # cookies = {
    #     "ksq_region": "2",
    #     "hist_region": "2",
    # }
    cookies = {}

    try:
        response = requests.get(url, headers=headers, cookies=cookies)
        logging.info(f"Response status code: {response.status_code}")

        if response.status_code != 200:
            logging.error(f"Failed to fetch data: {response.status_code}")
            return []

        # Check response headers
        logging.info(f"Response headers: {dict(response.headers)}")

        # Check if response has content
        if not response.content:
            logging.error("Empty response from API")
            return []

        # Try to decode the response properly
        try:
            # First try to get text (requests should handle decompression)
            response_text = response.text
            logging.info(f"Response text length: {len(response_text)}")
            logging.info(f"Response text preview: {response_text[:200]}...")
        except UnicodeDecodeError as e:
            logging.error(f"Failed to decode response as text: {e}")
            # Try to decode as UTF-8 manually
            try:
                response_text = response.content.decode("utf-8")
                logging.info(f"Manually decoded response length: {len(response_text)}")
                logging.info(f"Manually decoded preview: {response_text[:200]}...")
            except UnicodeDecodeError:
                logging.error("Failed to decode response even manually")
                return []

        # Check if response is empty after decoding
        if not response_text.strip():
            logging.error("Empty response text after decoding")
            return []

        # Parse JSON response
        try:
            data = response.json()
        except ValueError as e:
            logging.error(f"Failed to parse JSON response: {e}")
            logging.error(f"Response content: {response_text[:500]}")
            return []

        if not isinstance(data, list):
            logging.error(f"Expected list, got {type(data)}: {data}")
            return []

        logging.info(f"Successfully fetched {len(data)} residential complexes")
        # logging.info(data)

        # Debug: print first few items to understand structure
        if data and len(data) > 0:
            logging.info("Sample data structure:")
            logging.info(f"   First item: {data[0]}")
            if len(data) > 1:
                logging.info(f"   Second item: {data[1]}")

        return data

    except Exception as e:
        logging.error(f"Error fetching residential complexes: {e}")
        return []


def parse_complex_data(
    complexes_data: List[Dict], list_of_jks_to_skip: List[str] = None
) -> List[Dict]:
    """
    Parse and clean residential complex data.

    :param complexes_data: List[Dict], raw complex data from API
    :param list_of_jks_to_skip: Optional[List[str]], list of complex_ids to skip (JKs that already have valid cities)
    :return: List[Dict], cleaned complex data
    """
    cleaned_complexes = []
    skip_set = set(list_of_jks_to_skip) if list_of_jks_to_skip else set()
    skipped_count = 0

    for complex_data in complexes_data:
        try:
            # The API returns data with 'key' and 'value' fields
            # Extract complex information from the structure
            complex_id = complex_data.get("key", "")
            name = complex_data.get("value", "").strip()

            # Skip if no valid ID or name
            if not complex_id or not name or complex_id == "" or name == "":
                continue

            # Skip if this JK already has a valid city
            complex_id_str = str(complex_id)
            if complex_id_str in skip_set:
                skipped_count += 1
                continue

            # Extract city and district if available (not present in this API)
            city = None
            district = None

            # If city/district not in separate fields, try to extract from name
            if "," in name:
                parts = name.split(",")
                if len(parts) >= 2:
                    name = parts[0].strip()
                    location = parts[1].strip()
                    if location:
                        # Try to extract city and district
                        location_parts = location.split()
                        if len(location_parts) >= 2:
                            city = location_parts[0]
                            district = " ".join(location_parts[1:])
                        else:
                            city = location

            cleaned_complexes.append(
                {
                    "complex_id": complex_id_str,
                    "name": name,
                    "city": city,
                    "district": district,
                }
            )

        except Exception as e:
            logging.info(f"Error parsing complex data: {e}")
            continue

    logging.info(
        f"Parsed {len(cleaned_complexes)} valid complexes (skipped {skipped_count} with existing cities)"
    )
    return cleaned_complexes


def get_city_from_jk_sales(complex_id: str, jk_name: str) -> str:
    """
    Get city for a JK by scraping the first page of sales and extracting city from sales ads.

    :param complex_id: str, Krisha complex ID
    :param jk_name: str, name of the residential complex (for logging)
    :return: str, city name or "Unknown" if not found
    """
    try:
        # Construct search URL for first page
        search_url = f"https://krisha.kz/prodazha/kvartiry/almaty/?das[map.complex]={complex_id}&page=1"

        # Get flat URLs from first page
        flat_urls = get_flat_urls_from_search_page(search_url)

        if not flat_urls:
            logging.info(f"No sales found for {jk_name}, setting city to Unknown")
            return "Unknown"

        # Try to get city from first few flats (up to 3)
        cities_found = []
        for flat_url in flat_urls[:3]:
            try:
                flat_id = extract_flat_id_from_url(flat_url)
                if not flat_id:
                    continue

                # Get city from analytics API
                api_url = f"https://m.krisha.kz/analytics/aPriceAnalysis/?id={flat_id}"
                headers = {
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
                    "Accept": "application/json, text/plain, */*",
                    "Referer": f"https://m.krisha.kz/a/show/{flat_id}",
                    "Origin": "https://m.krisha.kz",
                }

                response = requests.get(api_url, headers=headers, timeout=10)
                response.raise_for_status()

                data = response.json()
                city_name = data.get("cityName")
                if city_name:
                    cities_found.append(city_name)

                # Small delay to be respectful
                time.sleep(0.5)

            except Exception as e:
                logging.debug(f"Could not get city from flat {flat_id}: {e}")
                continue

        if cities_found:
            # Return the most common city (or first one if all are different)
            city = max(set(cities_found), key=cities_found.count)
            logging.info(f"Found city '{city}' for {jk_name} from sales ads")
            return city
        else:
            logging.info(
                f"Could not extract city from sales for {jk_name}, setting to Unknown"
            )
            return "Unknown"

    except Exception as e:
        logging.warning(f"Error getting city for {jk_name}: {e}")
        return "Unknown"


def get_city_from_jk_rentals(complex_id: str, jk_name: str) -> str:
    """
    Get city for a JK by scraping the first page of rentals and extracting city from rental ads.

    :param complex_id: str, Krisha complex ID
    :param jk_name: str, name of the residential complex (for logging)
    :return: str, city name or "Unknown" if not found
    """
    try:
        # Construct search URL for first page
        search_url = f"https://krisha.kz/arenda/kvartiry/almaty/?das[map.complex]={complex_id}&page=1"

        # Get flat URLs from first page
        flat_urls = get_flat_urls_from_search_page(search_url)

        if not flat_urls:
            logging.info(f"No rentals found for {jk_name}, setting city to Unknown")
            return "Unknown"

        # Try to get city from first few flats (up to 3)
        cities_found = []
        for flat_url in flat_urls[:3]:
            try:
                flat_id = extract_flat_id_from_url(flat_url)
                if not flat_id:
                    continue

                # Get city from analytics API
                api_url = f"https://m.krisha.kz/analytics/aPriceAnalysis/?id={flat_id}"
                headers = {
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
                    "Accept": "application/json, text/plain, */*",
                    "Referer": f"https://m.krisha.kz/a/show/{flat_id}",
                    "Origin": "https://m.krisha.kz",
                }

                response = requests.get(api_url, headers=headers, timeout=10)
                response.raise_for_status()

                data = response.json()
                city_name = data.get("cityName")
                if city_name:
                    cities_found.append(city_name)

                # Small delay to be respectful
                time.sleep(0.5)

            except Exception as e:
                logging.debug(f"Could not get city from flat {flat_id}: {e}")
                continue

        if cities_found:
            # Return the most common city (or first one if all are different)
            city = max(set(cities_found), key=cities_found.count)
            logging.info(f"Found city '{city}' for {jk_name} from rental ads")
            return city
        else:
            logging.info(
                f"Could not extract city from rentals for {jk_name}, setting to Unknown"
            )
            return "Unknown"

    except Exception as e:
        logging.warning(f"Error getting city from rentals for {jk_name}: {e}")
        return "Unknown"


def process_and_save_complexes(complexes: List[Dict], db_path: str = "flats.db") -> int:
    """
    Process and save residential complexes to database.
    Adds or updates complexes depending on city info.
    """
    if not complexes:
        logging.info("No complexes to save")
        return 0

    db = OrthancDB(db_path)
    saved, skipped = 0, 0

    def resolve_city(cid: str, name: str, given_city: Optional[str]) -> str:
        """Try to determine city if missing or Unknown."""
        if given_city and given_city != "Unknown":
            return given_city
        for fn in (get_city_from_jk_sales, get_city_from_jk_rentals):
            city = fn(cid, name)
            if city and city != "Unknown":
                return city
        return "Unknown"

    for i, data in enumerate(complexes, 1):
        try:
            cid, name, district = data["complex_id"], data["name"], data.get("district")
            existing = db.get_residential_complex_by_complex_id(cid)

            if existing:
                city = existing.get("city")
                if city and city != "Unknown":
                    logging.info(f"[{i}/{len(complexes)}] Skip {name} – city={city}")
                    skipped += 1
                    continue

                new_city = resolve_city(cid, name, data.get("city"))
                if db.update_residential_complex_city_and_district(
                    cid, new_city, district
                ):
                    saved += 1
                    logging.info(
                        f"[{i}/{len(complexes)}] Updated {name} – city={new_city}"
                    )

            else:
                city = resolve_city(cid, name, data.get("city"))
                if db.insert_residential_complex_new(cid, name, city, district):
                    saved += 1
                    logging.info(f"[{i}/{len(complexes)}] Added {name} – city={city}")

        except Exception as e:
            logging.warning(
                f"Error processing {data.get('complex_id', 'unknown')}: {e}"
            )

    logging.info(
        f"Saved/updated {saved}/{len(complexes)} complexes (skipped {skipped})"
    )
    return saved


def update_jks_with_unknown_cities(db_path: str = "flats.db") -> int:
    """
    Update JKs with NULL or "Unknown" cities by scraping sales ads to get the city.

    :param db_path: str, database file path
    :return: int, number of JKs updated with city information
    """
    logging.info("Updating JKs with unknown cities...")

    db = OrthancDB(db_path)

    try:
        # Get all JKs with NULL or "Unknown" city
        jks_to_update = db.get_jks_with_unknown_cities()

        if not jks_to_update:
            logging.info("No JKs with unknown cities found")
            return 0

        logging.info(f"Found {len(jks_to_update)} JKs with unknown cities")

        updated_count = 0

        for i, jk in enumerate(jks_to_update, 1):
            complex_id = jk["complex_id"]
            jk_name = jk["name"]

            try:
                logging.info(
                    f"[{i}/{len(jks_to_update)}] Getting city for {jk_name}..."
                )
                city = get_city_from_jk_sales(complex_id, jk_name)

                # Update the city in database
                if db.update_jk_city(complex_id, city):
                    updated_count += 1

            except Exception as e:
                logging.warning(f"Error updating city for {jk_name}: {e}")
                continue

        logging.info(
            f"Updated {updated_count}/{len(jks_to_update)} JKs with city information"
        )
        return updated_count

    except Exception as e:
        logging.error(f"Error updating JKs with unknown cities: {e}")
        return 0


def search_complex_by_name(name: str, db_path: str = "flats.db") -> Optional[Dict]:
    """
    Search for a residential complex by name.

    :param name: str, complex name to search for
    :param db_path: str, database file path
    :return: Optional[Dict], complex information if found
    """
    db = OrthancDB(db_path)
    complexes = db.get_all_residential_complexes()

    # Case-insensitive search
    name_lower = name.lower()

    for complex_data in complexes:
        if name_lower in complex_data["name"].lower():
            return complex_data

    return None


def search_complexes_by_name(name: str, db_path: str = "flats.db") -> List[Dict]:
    """
    Search for residential complexes by name (returns multiple matches).

    :param name: str, complex name to search for
    :param db_path: str, database file path
    :return: List[Dict], list of matching complexes
    """
    db = OrthancDB(db_path)
    complexes = db.get_all_residential_complexes()

    # Case-insensitive search
    name_lower = name.lower()
    matches = []

    for complex_data in complexes:
        if name_lower in complex_data["name"].lower():
            matches.append(complex_data)

    return matches


def search_complexes_by_name_deduplicated(
    name: str, db_path: str = "flats.db"
) -> List[Dict]:
    """
    Search for residential complexes by name with deduplication of similar names.

    :param name: str, complex name to search for
    :param db_path: str, database file path
    :return: List[Dict], list of deduplicated matching complexes
    """
    db = OrthancDB(db_path)
    complexes = db.get_all_residential_complexes()

    # Case-insensitive search
    name_lower = name.lower()
    matches = []

    for complex_data in complexes:
        if name_lower in complex_data["name"].lower():
            matches.append(complex_data)

    # Deduplicate similar names
    if len(matches) > 1:
        # Group by normalized name (remove common variations)
        normalized_groups = {}

        for complex_data in matches:
            # Normalize the name for grouping
            normalized_name = normalize_complex_name(complex_data["name"])

            if normalized_name not in normalized_groups:
                normalized_groups[normalized_name] = []
            normalized_groups[normalized_name].append(complex_data)

        # For each group, select the best representative
        deduplicated_matches = []
        for group_name, group_complexes in normalized_groups.items():
            if len(group_complexes) == 1:
                deduplicated_matches.append(group_complexes[0])
            else:
                # Multiple complexes with similar names - select the best one
                best_complex = select_best_complex_representative(group_complexes)
                deduplicated_matches.append(best_complex)

        return deduplicated_matches

    return matches


def normalize_complex_name(name: str) -> str:
    """
    Normalize complex name for deduplication.

    :param name: str, original complex name
    :return: str, normalized name
    """
    # Convert to lowercase
    normalized = name.lower()

    # Remove common suffixes and prefixes
    suffixes_to_remove = [
        " apartments",
        " apartment",
        " жк",
        " жилой комплекс",
        " residential complex",
        " complex",
        " квартал",
        " quarter",
    ]

    for suffix in suffixes_to_remove:
        if normalized.endswith(suffix):
            normalized = normalized[: -len(suffix)]

    # Remove extra spaces
    normalized = " ".join(normalized.split())

    return normalized


def select_best_complex_representative(complexes: List[Dict]) -> Dict:
    """
    Select the best representative from a group of similar complexes.

    :param complexes: List[Dict], list of similar complexes
    :return: Dict, the best representative
    """
    if len(complexes) == 1:
        return complexes[0]

    # Prefer complexes with more complete information
    scored_complexes = []

    for complex_data in complexes:
        score = 0

        # Prefer longer names (more descriptive)
        score += len(complex_data["name"])

        # Prefer names with proper capitalization
        if complex_data["name"].istitle() or complex_data["name"].isupper():
            score += 10

        # Prefer names that don't end with common suffixes
        name_lower = complex_data["name"].lower()
        if not any(
            name_lower.endswith(suffix)
            for suffix in [" apartments", " apartment", " жк"]
        ):
            score += 5

        # Prefer names that contain the search term more prominently
        if complex_data["name"].lower().startswith("meridian"):
            score += 20

        scored_complexes.append((score, complex_data))

    # Sort by score (highest first) and return the best
    scored_complexes.sort(key=lambda x: x[0], reverse=True)
    return scored_complexes[0][1]


def get_complex_by_id(complex_id: str, db_path: str = "flats.db") -> Optional[Dict]:
    """
    Get residential complex by ID.

    :param complex_id: str, complex ID
    :param db_path: str, database file path
    :return: Optional[Dict], complex information or None if not found
    """
    db = OrthancDB(db_path)
    return db.get_residential_complex_by_id(complex_id)


def get_all_residential_complexes(db_path: str = "flats.db") -> List[Dict]:
    """
    Get all residential complexes from database.

    :param db_path: str, database file path
    :return: List[Dict], list of all residential complexes
    """
    db = OrthancDB(db_path)
    return db.get_all_residential_complexes()


def update_complex_database(db_path: str = "flats.db") -> int:
    """
    Update the residential complex database by fetching fresh data.
    Only adds new JKs or updates existing ones if their city is NULL or "Unknown".

    :param db_path: str, database file path
    :return: int, number of complexes saved or updated
    """
    logging.info("Updating residential complex database...")

    # Read all existing JKs from database first
    db = OrthancDB(db_path)
    existing_jks = db.get_all_residential_complexes()

    # Separate JKs into two lists:
    # 1. JKs with valid cities (to skip during parsing)
    # 2. JKs without cities (to update)
    jks_with_city = []
    jks_without_city = []

    for jk in existing_jks:
        complex_id = str(jk.get("complex_id", ""))
        city = jk.get("city")
        if city and city != "Unknown":
            jks_with_city.append(complex_id)
        else:
            jks_without_city.append(complex_id)

    logging.info(
        f"Found {len(existing_jks)} existing JKs: {len(jks_with_city)} with cities, {len(jks_without_city)} without cities"
    )

    # Fetch complexes from API
    complexes_data = fetch_residential_complexes()

    if not complexes_data:
        logging.info("No complexes data received")
        return 0

    # Parse and clean data, skipping JKs that already have valid cities
    cleaned_complexes = parse_complex_data(
        complexes_data, list_of_jks_to_skip=jks_with_city
    )

    if not cleaned_complexes:
        logging.info("No valid complexes found")
        return 0

    # Process and save to database (only new JKs or those with NULL/Unknown cities)
    saved_count = process_and_save_complexes(cleaned_complexes, db_path)

    return saved_count
