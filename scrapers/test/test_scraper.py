"""
Test script for the Krisha.kz scraper.
"""
from common.src.krisha_scraper import scrape_flat_info


def test_scraper():
    """
    Test the scraper with a sample URL.
    """
    # Test URL from the example
    test_url = "https://krisha.kz/a/show/1003924251"
    
    print("Testing Krisha.kz scraper...")
    print(f"URL: {test_url}")
    print("-" * 50)
    
    try:
        # Scrape flat information
        flat_info = scrape_flat_info(test_url)
        
        # Print results
        print("✅ Successfully extracted flat information:")
        print(f"📋 Flat ID: {flat_info.flat_id}")
        print(f"💰 Price: {flat_info.price:,} tenge")
        print(f"📏 Area: {flat_info.area} m²")
        print(f"🏢 Residential Complex: {flat_info.residential_complex or 'N/A'}")
        print(f"🏠 Floor: {flat_info.floor}/{flat_info.total_floors if flat_info.floor else 'N/A'}")
        print(f"🏗️ Construction Year: {flat_info.construction_year or 'N/A'}")
        print(f"🚗 Parking: {flat_info.parking or 'N/A'}")
        print(f"📝 Description: {flat_info.description[:100]}...")
        
        # Validate data
        assert flat_info.flat_id == "1003924251"
        assert flat_info.price > 0
        assert flat_info.area > 0
        assert flat_info.description
        
        print("\n✅ All validations passed!")
        
    except Exception as e:
        print(f"❌ Error during scraping: {e}")
        raise


if __name__ == "__main__":
    test_scraper() 