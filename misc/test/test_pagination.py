
"""
Test script for pagination functionality.
"""
from scrapers.src.search_scraper import detect_pagination_info, generate_page_urls, analyze_search_page, \
    scrape_search_results_with_pagination
import logging

def test_pagination_detection():
    """Test pagination detection."""
    logging.info("Testing Pagination Detection")
    logging.info("=" * 50)
    
    # Test URL with broader search to get more results
    test_url = "https://krisha.kz/arenda/kvartiry/almaty/?das[live.rooms]=1"
    
    logging.info(f"Testing URL: {test_url}")
    
    # Detect pagination
    pagination_info = detect_pagination_info(test_url)
    
    logging.info(f"\nPagination Information:")
    for key, value in pagination_info.items():
        logging.info(f"   {key}: {value}")
    
    if pagination_info['has_pagination']:
        logging.info(f"\nPagination detected!")
        logging.info(f"   Will generate URLs for up to {pagination_info['max_page_found']} pages")
        
        # Generate page URLs
        page_urls = generate_page_urls(test_url, min(5, pagination_info['max_page_found']))
        
        logging.info(f"\nGenerated {len(page_urls)} page URLs:")
        for i, url in enumerate(page_urls, 1):
            logging.info(f"   Page {i}: {url}")
    else:
        logging.info(f"\nNo pagination detected - single page scraping")
    
    return pagination_info


def test_paginated_scraping():
    """Test paginated scraping."""
    logging.info("\nTesting Paginated Scraping")
    logging.info("=" * 50)
    
    # Test URL with broader search to get more results
    test_url = "https://krisha.kz/arenda/kvartiry/almaty/?das[live.rooms]=1"
    
    logging.info(f"Testing URL: {test_url}")
    
    # Analyze the search page first
    analysis = analyze_search_page(test_url)
    
    logging.info(f"\nSearch Page Analysis:")
    logging.info(f"   Total flats found: {analysis['total_flats_found']}")
    logging.info(f"   Pagination info: {analysis['pagination_info']}")
    
    # Test paginated scraping (limit to 2 pages and 10 flats for testing)
    logging.info(f"\nStarting paginated scraping (max 2 pages, max 10 flats)...")
    
    try:
        scraped_flats = scrape_search_results_with_pagination(
            search_url=test_url,
            max_pages=2,
            max_flats=10,
            delay=1.0  # Faster delay for testing
        )
        
        logging.info(f"\nPaginated scraping completed!")
        logging.info(f"   Successfully scraped {len(scraped_flats)} flats")
        
        if scraped_flats:
            logging.info(f"\nSample scraped flats:")
            for i, flat in enumerate(scraped_flats[:3], 1):  # Show first 3
                logging.info(f"   {i}. Flat {flat.flat_id}")
                logging.info(f"      Price: {flat.price:,} tenge")
                logging.info(f"      Area: {flat.area} m²")
                logging.info(f"      Complex: {flat.residential_complex or 'N/A'}")
        
        return scraped_flats
        
    except Exception as e:
        logging.info(f"Error during paginated scraping: {e}")
        return []


def test_url_generation():
    """Test URL generation for pagination."""
    logging.info("\nTesting URL Generation")
    logging.info("=" * 50)
    
    # Test different URL formats
    test_urls = [
        "https://krisha.kz/arenda/kvartiry/almaty/?das[live.rooms]=1",
        "https://krisha.kz/arenda/kvartiry/almaty/?das[live.rooms]=1&page=2",
        "https://krisha.kz/prodazha/kvartiry/almaty/?das[live.rooms]=2&das[map.complex]=2758"
    ]
    
    for i, test_url in enumerate(test_urls, 1):
        logging.info(f"\nTest URL {i}: {test_url}")
        
        # Generate page URLs
        page_urls = generate_page_urls(test_url, 3)
        
        logging.info(f"   Generated {len(page_urls)} page URLs:")
        for j, url in enumerate(page_urls, 1):
            logging.info(f"     Page {j}: {url}")


def main():
    """Main test function."""
    logging.info("Krisha.kz Pagination Test")
    logging.info("=" * 50)
    
    # Test URL generation
    test_url_generation()
    
    # Test pagination detection
    pagination_info = test_pagination_detection()
    
    # Test paginated scraping
    scraped_flats = test_paginated_scraping()
    
    logging.info(f"\nPagination test completed!")
    logging.info(f"   Pagination detected: {pagination_info['has_pagination']}")
    logging.info(f"   Flats scraped: {len(scraped_flats)}")


if __name__ == "__main__":
    main() 