"""
Search Scraper for Krisha.kz

This tool scrapes flat listings from search pages by extracting URLs
and then scraping individual flat information.
"""
from datetime import datetime

import requests
import re
import time
from typing import List, Dict, Optional
from urllib.parse import urljoin, urlparse, parse_qs, urlencode

from common.src.krisha_scraper import FlatInfo, scrape_flat_info
from db.src.enhanced_database import save_rental_flat_to_db, save_sales_flat_to_db


def detect_pagination_info(url: str) -> Dict:
    """
    Detect pagination information from a search page.
    
    :param url: str, search page URL
    :return: Dict, pagination information
    """
    print(f"🔍 Detecting pagination for: {url}")
    
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
        
        # Look for total results count
        total_results = None
        pagination_patterns = [
            r'(\d+)\s*объявлений?',
            r'(\d+)\s*предложений?',
            r'Найдено\s*<span>(\d+)</span>\s*объявлений?',
            r'Найдено\s*(\d+)\s*объявлений?',
            r'(\d+)\s*results?',
            r'(\d+)\s*items?',
        ]
        
        for pattern in pagination_patterns:
            match = re.search(pattern, html_content, re.IGNORECASE)
            if match:
                total_results = int(match.group(1))
                break
        
        # Look for current page
        current_page = 1
        page_patterns = [
            r'page=(\d+)',
            r'p=(\d+)',
            r'страница\s*(\d+)',
        ]
        
        for pattern in page_patterns:
            match = re.search(pattern, html_content)
            if match:
                current_page = int(match.group(1))
                break
        
        # Look for pagination links to determine total pages
        pagination_links = re.findall(r'href=["\']([^"\']*page=\d+[^"\']*)["\']', html_content)
        page_numbers = []
        
        for link in pagination_links:
            page_match = re.search(r'page=(\d+)', link)
            if page_match:
                page_numbers.append(int(page_match.group(1)))
        
        max_page = max(page_numbers) if page_numbers else 1
        
        # Estimate total pages based on results per page (typically 20-30)
        estimated_pages = None
        if total_results:
            results_per_page = 25  # Typical for Krisha.kz
            estimated_pages = (total_results + results_per_page - 1) // results_per_page
            
        # Use max_page_found if it's larger than estimated_pages (more accurate)
        if max_page > 1 and (estimated_pages is None or max_page > estimated_pages):
            estimated_pages = max_page
        
        return {
            'total_results': total_results,
            'current_page': current_page,
            'max_page_found': max_page,
            'estimated_pages': estimated_pages,
            'has_pagination': max_page > 1 or (total_results and total_results > 50)
        }
        
    except Exception as e:
        print(f"❌ Error detecting pagination: {e}")
        return {
            'total_results': None,
            'current_page': 1,
            'max_page_found': 1,
            'estimated_pages': None,
            'has_pagination': False
        }


def generate_page_urls(base_url: str, max_pages: int = 10) -> List[str]:
    """
    Generate URLs for multiple pages of search results.
    
    :param base_url: str, base search URL
    :param max_pages: int, maximum number of pages to generate
    :return: List[str], list of page URLs
    """
    urls = []
    
    # Parse the base URL
    parsed = urlparse(base_url)
    query_params = parse_qs(parsed.query)
    
    # Remove existing page parameter if present
    if 'page' in query_params:
        del query_params['page']
    
    # Generate URLs for each page
    for page in range(1, max_pages + 1):
        # Add page parameter
        page_params = query_params.copy()
        page_params['page'] = [str(page)]
        
        # Reconstruct URL
        new_query = urlencode(page_params, doseq=True)
        page_url = f"{parsed.scheme}://{parsed.netloc}{parsed.path}?{new_query}"
        urls.append(page_url)
    
    return urls


def extract_flat_urls_from_search_page(url: str) -> List[str]:
    """
    Extract flat URLs from a Krisha.kz search page.
    
    :param url: str, search page URL
    :return: List[str], list of flat URLs
    """
    print(f"🔍 Extracting flat URLs from: {url}")
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Mobile Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
    }
    
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        
        html_content = response.text
        
        # Look for flat URLs in the HTML
        flat_urls = []
        
        # Pattern for flat URLs: /a/show/ followed by numbers
        url_patterns = [
            r'href=["\'](/a/show/\d+)["\']',
            r'href=["\'](https?://krisha\.kz/a/show/\d+)["\']',
            r'href=["\'](https?://m\.krisha\.kz/a/show/\d+)["\']',
        ]
        
        for pattern in url_patterns:
            matches = re.findall(pattern, html_content)
            for match in matches:
                if match.startswith('http'):
                    flat_urls.append(match)
                else:
                    # Convert relative URL to absolute
                    flat_urls.append(urljoin('https://krisha.kz', match))
        
        # Remove duplicates while preserving order
        unique_urls = []
        seen = set()
        for url in flat_urls:
            if url not in seen:
                unique_urls.append(url)
                seen.add(url)
        
        print(f"✅ Found {len(unique_urls)} unique flat URLs")
        return unique_urls
        
    except Exception as e:
        print(f"❌ Error extracting URLs: {e}")
        return []


def scrape_search_results_with_pagination(search_url: str, max_pages: int = 5, max_flats: Optional[int] = None, delay: float = 2.0) -> List[FlatInfo]:
    """
    Scrape all flats from multiple pages of search results.
    
    :param search_url: str, base search page URL
    :param max_pages: int, maximum number of pages to scrape
    :param max_flats: Optional[int], maximum number of flats to scrape
    :param delay: float, delay between requests
    :return: List[FlatInfo], list of scraped flat information
    """
    print(f"🏠 Starting paginated scraping from: {search_url}")
    
    # Detect pagination information
    pagination_info = detect_pagination_info(search_url)
    
    if pagination_info['has_pagination']:
        print(f"📄 Pagination detected:")
        print(f"   Total results: {pagination_info['total_results']}")
        print(f"   Max page found: {pagination_info['max_page_found']}")
        print(f"   Estimated pages: {pagination_info['estimated_pages']}")
        
        # Determine how many pages to scrape
        # Prioritize max_page_found as it's more accurate than estimated_pages
        pages_to_scrape = min(max_pages, pagination_info['max_page_found'])
        
        # Only use estimated_pages if it's larger than max_page_found (rare case)
        if pagination_info['estimated_pages'] and pagination_info['estimated_pages'] > pagination_info['max_page_found']:
            pages_to_scrape = min(pages_to_scrape, pagination_info['estimated_pages'])
        
        print(f"📊 Will scrape {pages_to_scrape} pages")
        
        # Generate page URLs
        page_urls = generate_page_urls(search_url, pages_to_scrape)
        
        all_flat_urls = []
        
        # Extract URLs from each page
        for i, page_url in enumerate(page_urls, 1):
            print(f"\n📄 Scraping page {i}/{len(page_urls)}: {page_url}")
            
            page_urls = extract_flat_urls_from_search_page(page_url)
            all_flat_urls.extend(page_urls)
            
            print(f"   Found {len(page_urls)} flats on this page")
            print(f"   Total flats so far: {len(all_flat_urls)}")
            
            # Add delay between pages
            if i < len(page_urls):
                time.sleep(delay)
        
        # Remove duplicates
        unique_urls = []
        seen = set()
        for url in all_flat_urls:
            if url not in seen:
                unique_urls.append(url)
                seen.add(url)
        
        print(f"\n✅ Total unique flats found across {len(page_urls)} pages: {len(unique_urls)}")
        
    else:
        print("📄 No pagination detected, scraping single page")
        unique_urls = extract_flat_urls_from_search_page(search_url)
    
    if not unique_urls:
        print("❌ No flat URLs found")
        return []
    
    # No limit on flats - scrape all available
    print(f"📊 Scraping all {len(unique_urls)} available flats")
    
    print(f"\n🏠 Starting to scrape {len(unique_urls)} flats...")
    
    scraped_flats = []
    
    for i, url in enumerate(unique_urls, 1):
        print(f"\n[{i}/{len(unique_urls)}] Scraping: {url}")
        
        try:
            flat_info = scrape_flat_info(url)
            scraped_flats.append(flat_info)
            
            print(f"✅ Successfully scraped flat {flat_info.flat_id}")
            print(f"   Price: {flat_info.price:,} tenge")
            print(f"   Area: {flat_info.area} m²")
            print(f"   Residential Complex: {flat_info.residential_complex or 'N/A'}")
            
        except Exception as e:
            print(f"❌ Error scraping {url}: {e}")
        
        # Add delay between requests
        if i < len(unique_urls):
            time.sleep(delay)
    
    print(f"\n✅ Completed! Successfully scraped {len(scraped_flats)}/{len(unique_urls)} flats")
    return scraped_flats


def scrape_search_results(search_url: str, max_flats: Optional[int] = None, delay: float = 2.0) -> List[FlatInfo]:
    """
    Scrape all flats from a search page (single page, for backward compatibility).
    
    :param search_url: str, search page URL
    :param max_flats: Optional[int], maximum number of flats to scrape
    :param delay: float, delay between requests
    :return: List[FlatInfo], list of scraped flat information
    """
    # Extract flat URLs from search page
    flat_urls = extract_flat_urls_from_search_page(search_url)
    
    if not flat_urls:
        print("❌ No flat URLs found")
        return []
    
    # No limit on flats - scrape all available
    print(f"📊 Scraping all {len(flat_urls)} available flats")
    
    print(f"\n🏠 Starting to scrape {len(flat_urls)} flats...")
    
    scraped_flats = []
    
    for i, url in enumerate(flat_urls, 1):
        print(f"\n[{i}/{len(flat_urls)}] Scraping: {url}")
        
        try:
            flat_info = scrape_flat_info(url)
            scraped_flats.append(flat_info)
            
            print(f"✅ Successfully scraped flat {flat_info.flat_id}")
            print(f"   Price: {flat_info.price:,} tenge")
            print(f"   Area: {flat_info.area} m²")
            print(f"   Residential Complex: {flat_info.residential_complex or 'N/A'}")
            
        except Exception as e:
            print(f"❌ Error scraping {url}: {e}")
        
        # Add delay between requests
        if i < len(flat_urls):
            time.sleep(delay)
    
    print(f"\n✅ Completed! Successfully scraped {len(scraped_flats)}/{len(flat_urls)} flats")
    return scraped_flats


def scrape_and_save_search_results_with_pagination(search_url: str, db_path: str = "flats.db", 
                                                 max_pages: int = 5, max_flats: Optional[int] = None, delay: float = 2.0) -> List[FlatInfo]:
    """
    Scrape search results with pagination and save to database.
    
    :param search_url: str, base search page URL
    :param db_path: str, database file path
    :param max_pages: int, maximum number of pages to scrape
    :param max_flats: Optional[int], maximum number of flats to scrape
    :param delay: float, delay between requests
    :return: List[FlatInfo], list of scraped flat information
    """

    print(f"🏠 Starting paginated scraping and saving from: {search_url}")
    
    # Detect pagination information
    pagination_info = detect_pagination_info(search_url)
    
    if pagination_info['has_pagination']:
        print(f"📄 Pagination detected:")
        print(f"   Total results: {pagination_info['total_results']}")
        print(f"   Max page found: {pagination_info['max_page_found']}")
        print(f"   Estimated pages: {pagination_info['estimated_pages']}")
        
        # Determine how many pages to scrape
        # Prioritize max_page_found as it's more accurate than estimated_pages
        pages_to_scrape = min(max_pages, pagination_info['max_page_found'])
        
        # Only use estimated_pages if it's larger than max_page_found (rare case)
        if pagination_info['estimated_pages'] and pagination_info['estimated_pages'] > pagination_info['max_page_found']:
            pages_to_scrape = min(pages_to_scrape, pagination_info['estimated_pages'])
        
        print(f"📊 Will scrape {pages_to_scrape} pages")
        
        # Generate page URLs
        page_urls = generate_page_urls(search_url, pages_to_scrape)
        
        all_flat_urls = []
        
        # Extract URLs from each page
        for i, page_url in enumerate(page_urls, 1):
            print(f"\n📄 Scraping page {i}/{len(page_urls)}: {page_url}")
            
            page_urls = extract_flat_urls_from_search_page(page_url)
            all_flat_urls.extend(page_urls)
            
            print(f"   Found {len(page_urls)} flats on this page")
            print(f"   Total flats so far: {len(all_flat_urls)}")
            
            # Add delay between pages
            if i < len(page_urls):
                time.sleep(delay)
        
        # Remove duplicates
        unique_urls = []
        seen = set()
        for url in all_flat_urls:
            if url not in seen:
                unique_urls.append(url)
                seen.add(url)
        
        print(f"\n✅ Total unique flats found across {len(page_urls)} pages: {len(unique_urls)}")
        
    else:
        print("📄 No pagination detected, scraping single page")
        unique_urls = extract_flat_urls_from_search_page(search_url)
    
    if not unique_urls:
        print("❌ No flat URLs found")
        return []
    
    # No limit on flats - scrape all available
    print(f"📊 Scraping all {len(unique_urls)} available flats")
    
    print(f"\n�� Starting to scrape and save {len(unique_urls)} flats...")
    
    scraped_flats = []
    query_date = datetime.now().strftime('%Y-%m-%d')
    
    for i, url in enumerate(unique_urls, 1):
        print(f"\n[{i}/{len(unique_urls)}] Scraping: {url}")
        
        try:
            flat_info = scrape_flat_info(url)
            
            # Determine if it's rental or sale based on the original search URL
            if 'arenda' in search_url.lower():
                success = save_rental_flat_to_db(flat_info, url, query_date, db_path)
                print(f"success={success}")
                flat_type = "rental"
            else:
                success = save_sales_flat_to_db(flat_info, url, query_date, db_path)
                print(f"success={success}")
                flat_type = "sale"
            
            if success:
                scraped_flats.append(flat_info)
                print(f"✅ Successfully scraped and saved {flat_type} flat {flat_info.flat_id}")
                print(f"   Price: {flat_info.price:,} tenge")
                print(f"   Area: {flat_info.area} m²")
                print(f"   Residential Complex: {flat_info.residential_complex or 'N/A'}")
            else:
                print(f"⚠️ Failed to save flat {flat_info.flat_id}")
            
        except Exception as e:
            print(f"❌ Error scraping {url}: {e}")
        
        # Add delay between requests
        if i < len(unique_urls):
            time.sleep(delay)
    
    print(f"\n✅ Completed! Successfully scraped and saved {len(scraped_flats)}/{len(unique_urls)} flats")
    return scraped_flats


def scrape_and_save_search_results(search_url: str, db_path: str = "flats.db", 
                                 max_flats: Optional[int] = None, delay: float = 2.0) -> List[FlatInfo]:
    """
    Scrape search results and save to database (single page, for backward compatibility).
    
    :param search_url: str, search page URL
    :param db_path: str, database file path
    :param max_flats: Optional[int], maximum number of flats to scrape
    :param delay: float, delay between requests
    :return: List[FlatInfo], list of scraped flat information
    """
    # Extract flat URLs from search page
    flat_urls = extract_flat_urls_from_search_page(search_url)
    
    if not flat_urls:
        print("❌ No flat URLs found")
        return []
    
    # No limit on flats - scrape all available
    print(f"📊 Scraping all {len(flat_urls)} available flats")
    
    print(f"\n�� Starting to scrape and save {len(flat_urls)} flats...")
    
    scraped_flats = []
    query_date = datetime.now().strftime('%Y-%m-%d')
    
    for i, url in enumerate(flat_urls, 1):
        print(f"\n[{i}/{len(flat_urls)}] Scraping: {url}")
        
        try:
            flat_info = scrape_flat_info(url)
            
            # Determine if it's rental or sale based on the original search URL
            if 'arenda' in search_url.lower():
                success = save_rental_flat_to_db(flat_info, url, query_date, db_path)
                flat_type = "rental"
            else:
                success = save_sales_flat_to_db(flat_info, url, query_date, db_path)
                flat_type = "sale"
            
            if success:
                scraped_flats.append(flat_info)
                print(f"✅ Successfully scraped and saved {flat_type} flat {flat_info.flat_id}")
                print(f"   Price: {flat_info.price:,} tenge")
                print(f"   Area: {flat_info.area} m²")
                print(f"   Residential Complex: {flat_info.residential_complex or 'N/A'}")
            else:
                print(f"⚠️ Failed to save flat {flat_info.flat_id}")
            
        except Exception as e:
            print(f"❌ Error scraping {url}: {e}")
        
        # Add delay between requests
        if i < len(flat_urls):
            time.sleep(delay)
    
    print(f"\n✅ Completed! Successfully scraped and saved {len(scraped_flats)}/{len(flat_urls)} flats")
    return scraped_flats


def analyze_search_page(search_url: str) -> Dict:
    """
    Analyze a search page to understand its structure.
    
    :param search_url: str, search page URL
    :return: Dict, analysis results
    """
    print(f"🔍 Analyzing search page: {search_url}")
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Mobile Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
    }
    
    try:
        response = requests.get(search_url, headers=headers)
        response.raise_for_status()
        
        html_content = response.text
        
        # Extract flat URLs
        flat_urls = extract_flat_urls_from_search_page(search_url)
        
        # Get pagination information
        pagination_info = detect_pagination_info(search_url)
        
        return {
            'url': search_url,
            'total_flats_found': len(flat_urls),
            'pagination_info': pagination_info,
            'html_length': len(html_content),
            'flat_urls': flat_urls[:5],  # Show first 5 URLs as sample
        }
        
    except Exception as e:
        print(f"❌ Error analyzing page: {e}")
        return {'error': str(e)}


def main():
    """
    Main function to demonstrate search scraping.
    """
    # Test search URL
    search_url = "https://krisha.kz/arenda/kvartiry/almaty/?das[live.rooms]=1&das[live.square][to]=35&das[map.complex]=2758"
    
    print("🏠 Krisha.kz Search Scraper with Pagination")
    print("=" * 50)
    
    # Analyze the search page
    analysis = analyze_search_page(search_url)
    
    if 'error' in analysis:
        print(f"❌ Analysis failed: {analysis['error']}")
        return
    
    print(f"\n📊 Search Page Analysis:")
    print(f"Total flats found: {analysis['total_flats_found']}")
    print(f"Pagination info: {analysis['pagination_info']}")
    print(f"HTML length: {analysis['html_length']:,} characters")
    
    if analysis['flat_urls']:
        print(f"\n🔗 Sample flat URLs:")
        for i, url in enumerate(analysis['flat_urls'], 1):
            print(f"   {i}. {url}")
    
    # Ask user if they want to scrape
    print(f"\n💡 To scrape all flats from this search with pagination:")
    print(f"   python search_scraper.py scrape-paginated '{search_url}'")
    print(f"   python search_scraper.py scrape-save-paginated '{search_url}'")


if __name__ == "__main__":
    main() 