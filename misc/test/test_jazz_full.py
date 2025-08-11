
"""
Full test for Jazz-квартал pagination.
"""
import sys

from scrapers.src.search_scraper import detect_pagination_info, generate_page_urls, \
    scrape_search_results_with_pagination


def test_jazz_full_scraping():
    """Test full paginated scraping for Jazz-квартал."""
    print("🏠 Jazz-квартал Full Pagination Test")
    print("=" * 50)
    
    url = "https://krisha.kz/prodazha/kvartiry/almaty/?das[map.complex]=1206"
    
    print(f"🔍 Testing URL: {url}")
    
    # First, check pagination info
    pagination_info = detect_pagination_info(url)
    print(f"\n📊 Pagination Information:")
    print(f"   Total results: {pagination_info['total_results']}")
    print(f"   Max pages found: {pagination_info['max_page_found']}")
    print(f"   Estimated pages: {pagination_info['estimated_pages']}")
    print(f"   Has pagination: {pagination_info['has_pagination']}")
    
    if not pagination_info['has_pagination']:
        print("❌ No pagination detected!")
        return
    
    # Generate all page URLs
    max_pages = pagination_info['max_page_found']
    page_urls = generate_page_urls(url, max_pages)
    
    print(f"\n📄 Generated {len(page_urls)} page URLs")
    print(f"   Expected total flats: {pagination_info['total_results']}")
    print(f"   Expected flats per page: ~{pagination_info['total_results'] // max_pages}")
    
    # Test scraping with pagination (limit to first 3 pages for testing)
    print(f"\n🏠 Starting paginated scraping (max 3 pages for testing)...")
    
    try:
        scraped_flats = scrape_search_results_with_pagination(
            search_url=url,
            max_pages=3,  # Limit to 3 pages for testing
            max_flats=None,  # No limit on flats
            delay=1.0  # Faster delay for testing
        )
        
        print(f"\n✅ Paginated scraping completed!")
        print(f"   Successfully scraped {len(scraped_flats)} flats")
        
        if scraped_flats:
            print(f"\n📋 Sample scraped flats:")
            for i, flat in enumerate(scraped_flats[:5], 1):  # Show first 5
                print(f"   {i}. Flat {flat.flat_id}")
                print(f"      Price: {flat.price:,} tenge")
                print(f"      Area: {flat.area} m²")
                print(f"      Complex: {flat.residential_complex or 'N/A'}")
        
        # Calculate expected vs actual
        expected_flats = min(3 * 20, pagination_info['total_results'])  # 3 pages * 20 per page
        print(f"\n📊 Results Summary:")
        print(f"   Expected flats (3 pages): ~{expected_flats}")
        print(f"   Actual flats scraped: {len(scraped_flats)}")
        print(f"   Success rate: {len(scraped_flats)/expected_flats*100:.1f}%")
        
        return scraped_flats
        
    except Exception as e:
        print(f"❌ Error during paginated scraping: {e}")
        return []


def test_all_pages():
    """Test scraping all pages to get the full 178 flats."""
    print("\n🏠 Testing All Pages for Jazz-квартал")
    print("=" * 50)
    
    url = "https://krisha.kz/prodazha/kvartiry/almaty/?das[map.complex]=1206"
    
    # Get pagination info
    pagination_info = detect_pagination_info(url)
    max_pages = pagination_info['max_page_found']
    
    print(f"📊 Will scrape all {max_pages} pages to get all {pagination_info['total_results']} flats")
    
    try:
        scraped_flats = scrape_search_results_with_pagination(
            search_url=url,
            max_pages=max_pages,  # All pages
            max_flats=None,  # No limit
            delay=1.0  # Faster delay
        )
        
        print(f"\n✅ Full paginated scraping completed!")
        print(f"   Successfully scraped {len(scraped_flats)} flats")
        print(f"   Expected: {pagination_info['total_results']} flats")
        print(f"   Success rate: {len(scraped_flats)/pagination_info['total_results']*100:.1f}%")
        
        return scraped_flats
        
    except Exception as e:
        print(f"❌ Error during full paginated scraping: {e}")
        return []


def main():
    """Main test function."""
    print("🏠 Jazz-квартал Full Pagination Test")
    print("=" * 50)
    
    # Test first 3 pages
    scraped_flats = test_jazz_full_scraping()
    
    # Ask if user wants to test all pages
    print(f"\n💡 To test all pages and get all 178 flats:")
    print(f"   python test_jazz_full.py all")
    
    # Check if user wants to test all pages
    if len(sys.argv) > 1 and sys.argv[1] == 'all':
        print(f"\n🚀 Testing all pages...")
        all_flats = test_all_pages()
        print(f"\n✅ Full test completed!")
        print(f"   Total flats scraped: {len(all_flats)}")


if __name__ == "__main__":
    main() 