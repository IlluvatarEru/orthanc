"""
Example usage of the Krisha.kz scraper.

This script demonstrates how to use the scraper to extract flat information.
"""

from krisha_scraper import scrape_flat_info, FlatInfo


def main():
    """
    Example usage of the Krisha.kz scraper.
    """
    # Example URLs to test
    test_urls = [
        "https://krisha.kz/a/show/1003924251",
        # Add more URLs here for testing
    ]
    
    print("Krisha.kz Scraper Example")
    print("=" * 50)
    
    for i, url in enumerate(test_urls, 1):
        print(f"\n{i}. Processing: {url}")
        print("-" * 40)
        
        try:
            # Scrape flat information
            flat_info = scrape_flat_info(url)
            
            # Display results
            print(f"✅ Successfully extracted information:")
            print(f"   📋 Flat ID: {flat_info.flat_id}")
            print(f"   💰 Price: {flat_info.price:,} tenge")
            print(f"   📏 Area: {flat_info.area} m²")
            print(f"   🏢 Residential Complex: {flat_info.residential_complex or 'N/A'}")
            print(f"   🏠 Floor: {flat_info.floor}/{flat_info.total_floors if flat_info.floor else 'N/A'}")
            print(f"   🏗️ Construction Year: {flat_info.construction_year or 'N/A'}")
            print(f"   🚗 Parking: {flat_info.parking or 'N/A'}")
            print(f"   📝 Description: {flat_info.description[:100]}...")
            
        except Exception as e:
            print(f"❌ Error processing {url}: {e}")
    
    print("\n" + "=" * 50)
    print("Example completed!")


if __name__ == "__main__":
    main() 