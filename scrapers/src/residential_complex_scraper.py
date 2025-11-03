"""
Residential Complex Scraper for Krisha.kz

This module fetches residential complex data from Krisha.kz API
and stores it in the database for mapping complex IDs to names.
"""

from typing import List, Dict, Optional

import requests

from db.src.write_read_database import OrthancDB
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


def parse_complex_data(complexes_data: List[Dict]) -> List[Dict]:
    """
    Parse and clean residential complex data.

    :param complexes_data: List[Dict], raw complex data from API
    :return: List[Dict], cleaned complex data
    """
    cleaned_complexes = []

    for complex_data in complexes_data:
        try:
            # The API returns data with 'key' and 'value' fields
            # Extract complex information from the structure
            complex_id = complex_data.get("key", "")
            name = complex_data.get("value", "").strip()

            # Skip if no valid ID or name
            if not complex_id or not name or complex_id == "" or name == "":
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
                    "complex_id": str(complex_id),  # Ensure it's a string
                    "name": name,
                    "city": city,
                    "district": district,
                }
            )

        except Exception as e:
            logging.info(f"Error parsing complex data: {e}")
            continue

    logging.info(f"Parsed {len(cleaned_complexes)} valid complexes")
    return cleaned_complexes


def save_complexes_to_db(complexes: List[Dict], db_path: str = "flats.db") -> int:
    """
    Save residential complexes to database using a single connection.

    :param complexes: List[Dict], list of complex data
    :param db_path: str, database file path
    :return: int, number of complexes saved
    """
    if not complexes:
        logging.info("No complexes to save")
        return 0

    db = OrthancDB(db_path)
    db.connect()

    saved_count = 0

    try:
        for complex_data in complexes:
            try:
                # Use INSERT OR REPLACE to handle duplicates
                cursor = db.conn.execute(
                    """
                    INSERT OR REPLACE INTO residential_complexes 
                    (complex_id, name, city, district) 
                    VALUES (?, ?, ?, ?)
                """,
                    (
                        complex_data["complex_id"],
                        complex_data["name"],
                        complex_data.get("city"),
                        complex_data.get("district"),
                    ),
                )

                saved_count += 1

            except Exception as e:
                logging.info(
                    f"Error saving complex {complex_data.get('complex_id', 'unknown')}: {e}"
                )

        # Commit all changes at once
        db.conn.commit()
        logging.info(f"Saved {saved_count}/{len(complexes)} complexes to database")

    except Exception as e:
        logging.error(f"Error saving complexes to database: {e}")
        db.conn.rollback()
    finally:
        db.disconnect()

    return saved_count


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

    :param db_path: str, database file path
    :return: int, number of complexes updated
    """
    logging.info("Updating residential complex database...")

    # Fetch complexes from API
    complexes_data = fetch_residential_complexes()

    if not complexes_data:
        logging.info("No complexes data received")
        return 0

    # Parse and clean data
    cleaned_complexes = parse_complex_data(complexes_data)

    if not cleaned_complexes:
        logging.info("No valid complexes found")
        return 0

    # Save to database
    saved_count = save_complexes_to_db(cleaned_complexes, db_path)

    return saved_count


def main():
    """
    Main function to demonstrate complex scraping.
    """
    logging.info("🏢 Krisha.kz Residential Complex Scraper")
    logging.info("=" * 50)

    # Update complex database
    saved_count = update_complex_database()

    if saved_count > 0:
        logging.info(f"\nSuccessfully updated {saved_count} residential complexes")

        # Show some examples
        db = EnhancedFlatDatabase()
        complexes = db.get_all_residential_complexes()

        logging.info("\nSample complexes:")
        for i, complex_data in enumerate(complexes[:10], 1):
            logging.info(
                f"   {i}. {complex_data['name']} (ID: {complex_data['complex_id']})"
            )
            if complex_data.get("city"):
                logging.info(f"      City: {complex_data['city']}")

        # Test search functionality
        logging.info("\nTesting search functionality:")
        meridian = search_complex_by_name("Meridian")
        if meridian:
            logging.info(
                f"   Found Meridian: {meridian['name']} (ID: {meridian['complex_id']})"
            )
        else:
            logging.info("   Meridian not found")

    else:
        logging.info("Failed to update complex database")


if __name__ == "__main__":
    main()
