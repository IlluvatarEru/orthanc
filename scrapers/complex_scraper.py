"""
Residential Complex Scraper for Krisha.kz

This module fetches residential complex data from Krisha.kz API
and stores it in the database for mapping complex IDs to names.
"""

import requests
import json
from typing import List, Dict, Optional
from db.enhanced_database import EnhancedFlatDatabase


def fetch_residential_complexes() -> List[Dict]:
    """
    Fetch all residential complexes from Krisha.kz API.
    
    :return: List[Dict], list of residential complexes
    """
    print("🔍 Fetching residential complexes from Krisha.kz...")
    
    # API endpoint for residential complexes
    api_url = "https://krisha.kz/complex/ajaxMapComplexGetAll"
    
    # Parameters
    params = {
        'isSearch': '1'
    }
    
    # Headers to mimic browser
    headers = {
        'User-Agent': 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Mobile Safari/537.36',
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
        'X-Requested-With': 'XMLHttpRequest',
        'Referer': 'https://krisha.kz/',
        'Origin': 'https://krisha.kz',
    }
    
    try:
        response = requests.get(api_url, params=params, headers=headers)
        response.raise_for_status()
        
        # Parse JSON response
        data = response.json()
        
        print(f"✅ Successfully fetched {len(data)} residential complexes")
        
        # Debug: print first few items to understand structure
        if data and len(data) > 0:
            print(f"📋 Sample data structure:")
            print(f"   First item: {data[0]}")
            if len(data) > 1:
                print(f"   Second item: {data[1]}")
        
        return data
        
    except Exception as e:
        print(f"❌ Error fetching residential complexes: {e}")
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
            # The API returns data in a different format
            # Look for complexes with actual data (not empty placeholders)
            if complex_data.get('extra', {}).get('empty-value') == '1':
                continue  # Skip empty placeholders
            
            # Extract complex ID from key
            complex_id = complex_data.get('key', '')
            name = complex_data.get('value', '').strip()
            
            # Skip if no valid ID or name
            if not complex_id or not name or complex_id == '' or name == '&nbsp;':
                continue
            
            # Try to extract city/district from name if it contains them
            city = None
            district = None
            
            # Look for city/district patterns in the name
            if ',' in name:
                parts = name.split(',')
                if len(parts) >= 2:
                    name = parts[0].strip()
                    location = parts[1].strip()
                    if location:
                        # Try to extract city and district
                        location_parts = location.split()
                        if len(location_parts) >= 2:
                            city = location_parts[0]
                            district = ' '.join(location_parts[1:])
                        else:
                            city = location
            
            cleaned_complexes.append({
                'complex_id': complex_id,
                'name': name,
                'city': city,
                'district': district
            })
        
        except Exception as e:
            print(f"⚠️ Error parsing complex data: {e}")
            continue
    
    print(f"✅ Parsed {len(cleaned_complexes)} valid complexes")
    return cleaned_complexes


def save_complexes_to_db(complexes: List[Dict], db_path: str = "flats.db") -> int:
    """
    Save residential complexes to database.
    
    :param complexes: List[Dict], list of complex data
    :param db_path: str, database file path
    :return: int, number of complexes saved
    """
    db = EnhancedFlatDatabase(db_path)
    
    saved_count = 0
    
    for complex_data in complexes:
        try:
            success = db.insert_residential_complex(
                complex_id=complex_data['complex_id'],
                name=complex_data['name'],
                city=complex_data.get('city'),
                district=complex_data.get('district')
            )
            
            if success:
                saved_count += 1
                
        except Exception as e:
            print(f"⚠️ Error saving complex {complex_data.get('complex_id', 'unknown')}: {e}")
    
    print(f"✅ Saved {saved_count}/{len(complexes)} complexes to database")
    return saved_count


def search_complex_by_name(name: str, db_path: str = "flats.db") -> Optional[Dict]:
    """
    Search for a residential complex by name.
    
    :param name: str, complex name to search for
    :param db_path: str, database file path
    :return: Optional[Dict], complex information if found
    """
    db = EnhancedFlatDatabase(db_path)
    complexes = db.get_all_residential_complexes()
    
    # Case-insensitive search
    name_lower = name.lower()
    
    for complex_data in complexes:
        if name_lower in complex_data['name'].lower():
            return complex_data
    
    return None


def get_complex_by_id(complex_id: str, db_path: str = "flats.db") -> Optional[Dict]:
    """
    Get residential complex by ID.
    
    :param complex_id: str, complex ID
    :param db_path: str, database file path
    :return: Optional[Dict], complex information if found
    """
    db = EnhancedFlatDatabase(db_path)
    return db.get_residential_complex_by_id(complex_id)


def update_complex_database(db_path: str = "flats.db") -> int:
    """
    Update the residential complex database by fetching fresh data.
    
    :param db_path: str, database file path
    :return: int, number of complexes updated
    """
    print("🔄 Updating residential complex database...")
    
    # Fetch complexes from API
    complexes_data = fetch_residential_complexes()
    
    if not complexes_data:
        print("❌ No complexes data received")
        return 0
    
    # Parse and clean data
    cleaned_complexes = parse_complex_data(complexes_data)
    
    if not cleaned_complexes:
        print("❌ No valid complexes found")
        return 0
    
    # Save to database
    saved_count = save_complexes_to_db(cleaned_complexes, db_path)
    
    return saved_count


def main():
    """
    Main function to demonstrate complex scraping.
    """
    print("🏢 Krisha.kz Residential Complex Scraper")
    print("=" * 50)
    
    # Update complex database
    saved_count = update_complex_database()
    
    if saved_count > 0:
        print(f"\n✅ Successfully updated {saved_count} residential complexes")
        
        # Show some examples
        db = EnhancedFlatDatabase()
        complexes = db.get_all_residential_complexes()
        
        print(f"\n📋 Sample complexes:")
        for i, complex_data in enumerate(complexes[:10], 1):
            print(f"   {i}. {complex_data['name']} (ID: {complex_data['complex_id']})")
            if complex_data.get('city'):
                print(f"      City: {complex_data['city']}")
        
        # Test search functionality
        print(f"\n🔍 Testing search functionality:")
        meridian = search_complex_by_name("Meridian")
        if meridian:
            print(f"   Found Meridian: {meridian['name']} (ID: {meridian['complex_id']})")
        else:
            print("   Meridian not found")
    
    else:
        print("❌ Failed to update complex database")


if __name__ == "__main__":
    main() 