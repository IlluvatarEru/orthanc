
"""
Debug script for Jazz-квартал pagination issue.
"""
import requests
import re

from scrapers.src.search_scraper import detect_pagination_info, extract_flat_urls_from_search_page, generate_page_urls
import logging

def debug_jazz_page():
    """Debug the Jazz-квартал page specifically."""
    url = "https://krisha.kz/prodazha/kvartiry/almaty/?das[map.complex]=1206"
    
    logging.info("Debugging Jazz-квартал pagination")
    logging.info("=" * 50)
    logging.info(f"URL: {url}")
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Mobile Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
    }
    
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        
        html_content = response.text
        logging.info(f"Page loaded successfully")
        logging.info(f"   Content length: {len(html_content):,} characters")
        
        # Look for the total results count
        logging.info(f"\nSearching for total results count...")
        
        # Look for "178 объявлений" pattern
        results_patterns = [
            r'(\d+)\s*объявлений?',
            r'(\d+)\s*предложений?',
            r'Найдено\s*(\d+)\s*объявлений?',
            r'(\d+)\s*results?',
            r'(\d+)\s*items?',
        ]
        
        for pattern in results_patterns:
            matches = re.findall(pattern, html_content, re.IGNORECASE)
            if matches:
                logging.info(f"   Pattern '{pattern}' found: {matches}")
        
        # Look for pagination links
        logging.info(f"\nSearching for pagination links...")
        pagination_patterns = [
            r'href=["\']([^"\']*page=\d+[^"\']*)["\']',
            r'page=(\d+)',
            r'страница\s*(\d+)',
        ]
        
        for pattern in pagination_patterns:
            matches = re.findall(pattern, html_content)
            if matches:
                logging.info(f"   Pattern '{pattern}' found: {matches}")
        
        # Look for specific text that might indicate total count
        logging.info(f"\nLooking for specific text patterns...")
        if '178 объявлений' in html_content:
            logging.info("   Found '178 объявлений' in content")
        if 'Найдено 178 объявлений' in html_content:
            logging.info("   Found 'Найдено 178 объявлений' in content")
        
        # Show a sample of the content around these patterns
        logging.info(f"\nSample content around results count:")
        lines = html_content.split('\n')
        for i, line in enumerate(lines):
            if '178' in line or 'объявлений' in line or 'Найдено' in line:
                logging.info(f"   Line {i}: {line.strip()}")
        
        # Test the current pagination detection
        logging.info(f"\nTesting current pagination detection...")
        pagination_info = detect_pagination_info(url)
        logging.info(f"   Pagination info: {pagination_info}")
        
        # Test URL extraction
        logging.info(f"\nTesting URL extraction...")
        flat_urls = extract_flat_urls_from_search_page(url)
        logging.info(f"   Found {len(flat_urls)} flat URLs")
        
        # Test page URL generation
        logging.info(f"\nTesting page URL generation...")
        page_urls = generate_page_urls(url, 5)
        logging.info(f"   Generated {len(page_urls)} page URLs:")
        for i, page_url in enumerate(page_urls, 1):
            logging.info(f"     Page {i}: {page_url}")
        
        return html_content
        
    except Exception as e:
        logging.info(f"Error: {e}")
        return None


def test_jazz_pagination():
    """Test pagination for Jazz-квартал."""
    logging.info("\nTesting Jazz-квартал pagination")
    logging.info("=" * 50)
    
    url = "https://krisha.kz/prodazha/kvartiry/almaty/?das[map.complex]=1206"
    
    # Analyze the page
    analysis = analyze_search_page(url)
    
    logging.info(f"Analysis Results:")
    logging.info(f"   Total flats found: {analysis['total_flats_found']}")
    logging.info(f"   Pagination info: {analysis['pagination_info']}")
    
    # Test pagination detection
    pagination_info = detect_pagination_info(url)
    
    if pagination_info['has_pagination']:
        logging.info(f"\nPagination detected!")
        logging.info(f"   Max pages: {pagination_info['max_page_found']}")
        logging.info(f"   Total results: {pagination_info['total_results']}")
        
        # Generate URLs for first few pages
        page_urls = generate_page_urls(url, min(3, pagination_info['max_page_found']))
        
        logging.info(f"\nTesting first {len(page_urls)} pages:")
        total_urls = []
        
        for i, page_url in enumerate(page_urls, 1):
            logging.info(f"   Page {i}: {page_url}")
            page_urls = extract_flat_urls_from_search_page(page_url)
            total_urls.extend(page_urls)
            logging.info(f"     Found {len(page_urls)} flats")
        
        # Remove duplicates
        unique_urls = list(set(total_urls))
        logging.info(f"\nTotal unique flats across {len(page_urls)} pages: {len(unique_urls)}")
        
    else:
        logging.info(f"\nNo pagination detected")


def main():
    """Main debug function."""
    logging.info("Jazz-квартал Pagination Debug")
    logging.info("=" * 50)
    
    # Debug the page content
    html_content = debug_jazz_page()
    
    # Test pagination
    test_jazz_pagination()
    
    logging.info(f"\nDebug completed!")


if __name__ == "__main__":
    main() 