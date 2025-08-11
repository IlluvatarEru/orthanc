
"""
Test script for pagination functionality.
"""
from scrapers.src.search_scraper import detect_pagination_info, generate_page_urls


def test_pagination_detection():
    """Test pagination detection."""
    print("🧪 Testing Pagination Detection")
    print("=" * 50)
    
    # Test URL with broader search to get more results
    test_url = "https://krisha.kz/arenda/kvartiry/almaty/?das[live.rooms]=1"
    
    print(f"🔍 Testing URL: {test_url}")
    
    # Detect pagination
    pagination_info = detect_pagination_info(test_url)
    
    print(f"\n📊 Pagination Information:")
    for key, value in pagination_info.items():
        print(f"   {key}: {value}")
    
    if pagination_info['has_pagination']:
        print(f"\n✅ Pagination detected!")
        print(f"   Will generate URLs for up to {pagination_info['max_page_found']} pages")
        
        # Generate page URLs
        page_urls = generate_page_urls(test_url, min(5, pagination_info['max_page_found']))
        
        print(f"\n📄 Generated {len(page_urls)} page URLs:")
        for i, url in enumerate(page_urls, 1):
            print(f"   Page {i}: {url}")
    else:
        print(f"\n❌ No pagination detected - single page scraping")
    
    return pagination_info


def test_paginated_scraping():
    """Test paginated scraping."""
    print("\n🧪 Testing Paginated Scraping")
    print("=" * 50)
    
    # Test URL with broader search to get more results
    test_url = "https://krisha.kz/arenda/kvartiry/almaty/?das[live.rooms]=1"
    
    print(f"🔍 Testing URL: {test_url}")
    
    # Analyze the search page first
    analysis = analyze_search_page(test_url)
    
    print(f"\n📊 Search Page Analysis:")
    print(f"   Total flats found: {analysis['total_flats_found']}")
    print(f"   Pagination info: {analysis['pagination_info']}")
    
    # Test paginated scraping (limit to 2 pages and 10 flats for testing)
    print(f"\n🏠 Starting paginated scraping (max 2 pages, max 10 flats)...")
    
    try:
        scraped_flats = scrape_search_results_with_pagination(
            search_url=test_url,
            max_pages=2,
            max_flats=10,
            delay=1.0  # Faster delay for testing
        )
        
        print(f"\n✅ Paginated scraping completed!")
        print(f"   Successfully scraped {len(scraped_flats)} flats")
        
        if scraped_flats:
            print(f"\n📋 Sample scraped flats:")
            for i, flat in enumerate(scraped_flats[:3], 1):  # Show first 3
                print(f"   {i}. Flat {flat.flat_id}")
                print(f"      Price: {flat.price:,} tenge")
                print(f"      Area: {flat.area} m²")
                print(f"      Complex: {flat.residential_complex or 'N/A'}")
        
        return scraped_flats
        
    except Exception as e:
        print(f"❌ Error during paginated scraping: {e}")
        return []


def test_url_generation():
    """Test URL generation for pagination."""
    print("\n🧪 Testing URL Generation")
    print("=" * 50)
    
    # Test different URL formats
    test_urls = [
        "https://krisha.kz/arenda/kvartiry/almaty/?das[live.rooms]=1",
        "https://krisha.kz/arenda/kvartiry/almaty/?das[live.rooms]=1&page=2",
        "https://krisha.kz/prodazha/kvartiry/almaty/?das[live.rooms]=2&das[map.complex]=2758"
    ]
    
    for i, test_url in enumerate(test_urls, 1):
        print(f"\n🔗 Test URL {i}: {test_url}")
        
        # Generate page URLs
        page_urls = generate_page_urls(test_url, 3)
        
        print(f"   Generated {len(page_urls)} page URLs:")
        for j, url in enumerate(page_urls, 1):
            print(f"     Page {j}: {url}")


def main():
    """Main test function."""
    print("🏠 Krisha.kz Pagination Test")
    print("=" * 50)
    
    # Test URL generation
    test_url_generation()
    
    # Test pagination detection
    pagination_info = test_pagination_detection()
    
    # Test paginated scraping
    scraped_flats = test_paginated_scraping()
    
    print(f"\n✅ Pagination test completed!")
    print(f"   Pagination detected: {pagination_info['has_pagination']}")
    print(f"   Flats scraped: {len(scraped_flats)}")


if __name__ == "__main__":
    main() 