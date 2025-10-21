
"""
Full test for Jazz-квартал pagination.
"""
import sys

from scrapers.src.search_scraper import detect_pagination_info, generate_page_urls, \
    scrape_search_results_with_pagination
import logging

def test_jazz_full_scraping():
    """Test full paginated scraping for Jazz-квартал."""
    logging.info("Jazz-квартал Full Pagination Test")
    logging.info("=" * 50)
    
    url = "https://krisha.kz/prodazha/kvartiry/almaty/?das[map.complex]=1206"
    
    logging.info(f"Testing URL: {url}")
    
    # First, check pagination info
    pagination_info = detect_pagination_info(url)
    logging.info(f"\nPagination Information:")
    logging.info(f"   Total results: {pagination_info['total_results']}")
    logging.info(f"   Max pages found: {pagination_info['max_page_found']}")
    logging.info(f"   Estimated pages: {pagination_info['estimated_pages']}")
    logging.info(f"   Has pagination: {pagination_info['has_pagination']}")
    
    if not pagination_info['has_pagination']:
        logging.info("No pagination detected!")
        return
    
    # Generate all page URLs
    max_pages = pagination_info['max_page_found']
    page_urls = generate_page_urls(url, max_pages)
    
    logging.info(f"\nGenerated {len(page_urls)} page URLs")
    logging.info(f"   Expected total flats: {pagination_info['total_results']}")
    logging.info(f"   Expected flats per page: ~{pagination_info['total_results'] // max_pages}")
    
    # Test scraping with pagination (limit to first 3 pages for testing)
    logging.info(f"\nStarting paginated scraping (max 3 pages for testing)...")
    
    try:
        scraped_flats = scrape_search_results_with_pagination(
            search_url=url,
            max_pages=3,  # Limit to 3 pages for testing
            max_flats=None,  # No limit on flats
            delay=1.0  # Faster delay for testing
        )
        
        logging.info(f"\nPaginated scraping completed!")
        logging.info(f"   Successfully scraped {len(scraped_flats)} flats")
        
        if scraped_flats:
            logging.info(f"\nSample scraped flats:")
            for i, flat in enumerate(scraped_flats[:5], 1):  # Show first 5
                logging.info(f"   {i}. Flat {flat.flat_id}")
                logging.info(f"      Price: {flat.price:,} tenge")
                logging.info(f"      Area: {flat.area} m²")
                logging.info(f"      Complex: {flat.residential_complex or 'N/A'}")
        
        # Calculate expected vs actual
        expected_flats = min(3 * 20, pagination_info['total_results'])  # 3 pages * 20 per page
        logging.info(f"\nResults Summary:")
        logging.info(f"   Expected flats (3 pages): ~{expected_flats}")
        logging.info(f"   Actual flats scraped: {len(scraped_flats)}")
        logging.info(f"   Success rate: {len(scraped_flats)/expected_flats*100:.1f}%")
        
        return scraped_flats
        
    except Exception as e:
        logging.info(f"Error during paginated scraping: {e}")
        return []


def test_all_pages():
    """Test scraping all pages to get the full 178 flats."""
    logging.info("\nTesting All Pages for Jazz-квартал")
    logging.info("=" * 50)
    
    url = "https://krisha.kz/prodazha/kvartiry/almaty/?das[map.complex]=1206"
    
    # Get pagination info
    pagination_info = detect_pagination_info(url)
    max_pages = pagination_info['max_page_found']
    
    logging.info(f"Will scrape all {max_pages} pages to get all {pagination_info['total_results']} flats")
    
    try:
        scraped_flats = scrape_search_results_with_pagination(
            search_url=url,
            max_pages=max_pages,  # All pages
            max_flats=None,  # No limit
            delay=1.0  # Faster delay
        )
        
        logging.info(f"\nFull paginated scraping completed!")
        logging.info(f"   Successfully scraped {len(scraped_flats)} flats")
        logging.info(f"   Expected: {pagination_info['total_results']} flats")
        logging.info(f"   Success rate: {len(scraped_flats)/pagination_info['total_results']*100:.1f}%")
        
        return scraped_flats
        
    except Exception as e:
        logging.info(f"Error during full paginated scraping: {e}")
        return []


def main():
    """Main test function."""
    logging.info("Jazz-квартал Full Pagination Test")
    logging.info("=" * 50)
    
    # Test first 3 pages
    scraped_flats = test_jazz_full_scraping()
    
    # Ask if user wants to test all pages
    logging.info(f"\n💡 To test all pages and get all 178 flats:")
    logging.info(f"   python test_jazz_full.py all")
    
    # Check if user wants to test all pages
    if len(sys.argv) > 1 and sys.argv[1] == 'all':
        logging.info(f"\n🚀 Testing all pages...")
        all_flats = test_all_pages()
        logging.info(f"\nFull test completed!")
        logging.info(f"   Total flats scraped: {len(all_flats)}")


if __name__ == "__main__":
    main() 