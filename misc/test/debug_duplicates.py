
"""
Debug script to check for duplicates across pages.
"""
from scrapers.src.search_scraper import generate_page_urls, extract_flat_urls_from_search_page
import logging

def check_duplicates_across_pages():
    """Check for duplicates across different pages."""
    logging.info("Checking for duplicates across pages")
    logging.info("=" * 50)
    
    url = "https://krisha.kz/prodazha/kvartiry/almaty/?das[map.complex]=1206"
    
    # Generate page URLs
    page_urls = generate_page_urls(url, 5)  # First 5 pages
    
    logging.info(f"Testing first {len(page_urls)} pages:")
    
    all_urls = []
    page_urls_dict = {}
    
    for i, page_url in enumerate(page_urls, 1):
        logging.info(f"\nPage {i}: {page_url}")
        
        # Extract URLs from this page
        page_urls = extract_flat_urls_from_search_page(page_url)
        page_urls_dict[i] = page_urls
        all_urls.extend(page_urls)
        
        logging.info(f"   Found {len(page_urls)} flats on page {i}")
        
        # Show first few URLs from this page
        logging.info(f"   Sample URLs from page {i}:")
        for j, flat_url in enumerate(page_urls[:3], 1):
            flat_id = flat_url.split('/')[-1]
            logging.info(f"     {j}. Flat ID: {flat_id}")
    
    # Check for duplicates
    logging.info(f"\nAnalyzing duplicates...")
    logging.info(f"   Total URLs collected: {len(all_urls)}")
    
    # Count unique URLs
    unique_urls = list(set(all_urls))
    logging.info(f"   Unique URLs: {len(unique_urls)}")
    logging.info(f"   Duplicates found: {len(all_urls) - len(unique_urls)}")
    
    if len(all_urls) != len(unique_urls):
        logging.info(f"\nDUPLICATES DETECTED!")
        logging.info(f"   Duplicate rate: {(len(all_urls) - len(unique_urls)) / len(all_urls) * 100:.1f}%")
        
        # Find which URLs are duplicated
        from collections import Counter
        url_counts = Counter(all_urls)
        duplicates = {url: count for url, count in url_counts.items() if count > 1}
        
        logging.info(f"\nDuplicate URLs:")
        for url, count in list(duplicates.items())[:5]:  # Show first 5
            flat_id = url.split('/')[-1]
            logging.info(f"   Flat {flat_id}: appears {count} times")
    else:
        logging.info(f"\nNO DUPLICATES FOUND!")
    
    # Check if the issue is in the URL extraction function
    logging.info(f"\nChecking URL extraction function...")
    
    # Test the same page multiple times
    test_url = page_urls[0]  # First page
    logging.info(f"   Testing URL extraction on: {test_url}")
    
    for i in range(3):
        urls = extract_flat_urls_from_search_page(test_url)
        logging.info(f"   Run {i+1}: Found {len(urls)} URLs")
        if i == 0:
            first_run_urls = urls
        elif i == 1:
            second_run_urls = urls
            if first_run_urls == second_run_urls:
                logging.info(f"   Consistent results between runs")
            else:
                logging.info(f"   Inconsistent results between runs")
    
    return all_urls, unique_urls


def check_page_content():
    """Check the actual content of different pages."""
    logging.info(f"\nChecking page content differences")
    logging.info("=" * 50)
    
    url = "https://krisha.kz/prodazha/kvartiry/almaty/?das[map.complex]=1206"
    page_urls = generate_page_urls(url, 3)  # First 3 pages
    
    import requests
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Mobile Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
    }
    
    for i, page_url in enumerate(page_urls, 1):
        logging.info(f"\nPage {i}: {page_url}")
        
        try:
            response = requests.get(page_url, headers=headers)
            response.raise_for_status()
            
            html_content = response.text
            
            # Look for flat URLs in the HTML
            import re
            url_patterns = [
                r'href=["\'](/a/show/\d+)["\']',
                r'href=["\'](https?://krisha\.kz/a/show/\d+)["\']',
            ]
            
            all_matches = []
            for pattern in url_patterns:
                matches = re.findall(pattern, html_content)
                all_matches.extend(matches)
            
            # Convert relative URLs to absolute
            flat_urls = []
            for match in all_matches:
                if match.startswith('http'):
                    flat_urls.append(match)
                else:
                    flat_urls.append(f"https://krisha.kz{match}")
            
            # Remove duplicates
            unique_urls = list(set(flat_urls))
            
            logging.info(f"   Raw matches found: {len(all_matches)}")
            logging.info(f"   Unique flat URLs: {len(unique_urls)}")
            
            # Show sample flat IDs
            logging.info(f"   Sample flat IDs:")
            for j, flat_url in enumerate(unique_urls[:5], 1):
                flat_id = flat_url.split('/')[-1]
                logging.info(f"     {j}. {flat_id}")
                
        except Exception as e:
            logging.info(f"   Error: {e}")


def main():
    """Main debug function."""
    logging.info("Duplicate Detection Debug")
    logging.info("=" * 50)
    
    # Check for duplicates across pages
    all_urls, unique_urls = check_duplicates_across_pages()
    
    # Check page content
    check_page_content()
    
    logging.info(f"\nDebug completed!")
    logging.info(f"   Total URLs: {len(all_urls)}")
    logging.info(f"   Unique URLs: {len(unique_urls)}")
    logging.info(f"   Duplicate rate: {(len(all_urls) - len(unique_urls)) / len(all_urls) * 100:.1f}% if any")


if __name__ == "__main__":
    main() 