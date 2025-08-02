"""
Search Scraper for Krisha.kz

This tool scrapes flat listings from search pages by extracting URLs
and then scraping individual flat information.
"""

import requests
import re
import time
from typing import List, Dict, Optional
from urllib.parse import urljoin, urlparse
from common.krisha_scraper import scrape_flat_info, FlatInfo
from db.enhanced_database import EnhancedFlatDatabase


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


def scrape_search_results(search_url: str, max_flats: Optional[int] = None, delay: float = 2.0) -> List[FlatInfo]:
    """
    Scrape all flats from a search page.
    
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
    
    # Limit number of flats if specified
    if max_flats:
        flat_urls = flat_urls[:max_flats]
        print(f"📊 Limiting to {max_flats} flats")
    
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


def scrape_and_save_search_results(search_url: str, db_path: str = "flats.db", 
                                 max_flats: Optional[int] = None, delay: float = 2.0) -> List[FlatInfo]:
    """
    Scrape search results and save to database.
    
    :param search_url: str, search page URL
    :param db_path: str, database file path
    :param max_flats: Optional[int], maximum number of flats to scrape
    :param delay: float, delay between requests
    :return: List[FlatInfo], list of scraped flat information
    """
    # Import database functions
    from db.enhanced_database import save_rental_flat_to_db, save_sales_flat_to_db
    from datetime import datetime
    
    # Extract flat URLs from search page
    flat_urls = extract_flat_urls_from_search_page(search_url)
    
    if not flat_urls:
        print("❌ No flat URLs found")
        return []
    
    # Limit number of flats if specified
    if max_flats:
        flat_urls = flat_urls[:max_flats]
        print(f"📊 Limiting to {max_flats} flats")
    
    print(f"\n🏠 Starting to scrape and save {len(flat_urls)} flats...")
    
    scraped_flats = []
    
    for i, url in enumerate(flat_urls, 1):
        print(f"\n[{i}/{len(flat_urls)}] Processing: {url}")
        
        try:
            # Scrape flat information
            flat_info = scrape_flat_info(url)
            
            # Determine if this is rental or sales based on URL
            is_rental = 'arenda' in search_url.lower()
            query_date = datetime.now().strftime('%Y-%m-%d')
            
            # Save to database
            if is_rental:
                success = save_rental_flat_to_db(flat_info, url, query_date, db_path)
            else:
                success = save_sales_flat_to_db(flat_info, url, query_date, db_path)
            
            if success:
                print(f"✅ Successfully scraped and saved flat {flat_info.flat_id}")
                print(f"   Price: {flat_info.price:,} tenge")
                print(f"   Area: {flat_info.area} m²")
                print(f"   Residential Complex: {flat_info.residential_complex or 'N/A'}")
                scraped_flats.append(flat_info)
            else:
                print(f"⚠️ Failed to save flat {flat_info.flat_id} to database")
                scraped_flats.append(flat_info)
            
        except Exception as e:
            print(f"❌ Error processing {url}: {e}")
        
        # Add delay between requests
        if i < len(flat_urls):
            time.sleep(delay)
    
    print(f"\n✅ Completed! Successfully processed {len(scraped_flats)}/{len(flat_urls)} flats")
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
        
        # Look for pagination information
        pagination_patterns = [
            r'(\d+)\s*объявлений?',
            r'(\d+)\s*results?',
            r'(\d+)\s*items?',
        ]
        
        total_results = None
        for pattern in pagination_patterns:
            match = re.search(pattern, html_content, re.IGNORECASE)
            if match:
                total_results = int(match.group(1))
                break
        
        # Look for page numbers
        page_patterns = [
            r'page=(\d+)',
            r'p=(\d+)',
            r'страница\s*(\d+)',
        ]
        
        current_page = 1
        for pattern in page_patterns:
            match = re.search(pattern, html_content)
            if match:
                current_page = int(match.group(1))
                break
        
        return {
            'url': search_url,
            'total_flats_found': len(flat_urls),
            'total_results': total_results,
            'current_page': current_page,
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
    
    print("🏠 Krisha.kz Search Scraper")
    print("=" * 50)
    
    # Analyze the search page
    analysis = analyze_search_page(search_url)
    
    if 'error' in analysis:
        print(f"❌ Analysis failed: {analysis['error']}")
        return
    
    print(f"\n📊 Search Page Analysis:")
    print(f"Total flats found: {analysis['total_flats_found']}")
    print(f"Total results (if available): {analysis.get('total_results', 'Unknown')}")
    print(f"Current page: {analysis['current_page']}")
    print(f"HTML length: {analysis['html_length']:,} characters")
    
    if analysis['flat_urls']:
        print(f"\n🔗 Sample flat URLs:")
        for i, url in enumerate(analysis['flat_urls'], 1):
            print(f"   {i}. {url}")
    
    # Ask user if they want to scrape
    print(f"\n💡 To scrape all flats from this search:")
    print(f"   python search_scraper.py scrape '{search_url}'")
    print(f"   python search_scraper.py scrape-save '{search_url}'")


if __name__ == "__main__":
    main() 