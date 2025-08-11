
"""
Debug script for Jazz-квартал pagination issue.
"""
import requests
import re

from scrapers.src.search_scraper import detect_pagination_info, extract_flat_urls_from_search_page, generate_page_urls


def debug_jazz_page():
    """Debug the Jazz-квартал page specifically."""
    url = "https://krisha.kz/prodazha/kvartiry/almaty/?das[map.complex]=1206"
    
    print("🔍 Debugging Jazz-квартал pagination")
    print("=" * 50)
    print(f"URL: {url}")
    
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
        print(f"📄 Page loaded successfully")
        print(f"   Content length: {len(html_content):,} characters")
        
        # Look for the total results count
        print(f"\n🔍 Searching for total results count...")
        
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
                print(f"   Pattern '{pattern}' found: {matches}")
        
        # Look for pagination links
        print(f"\n🔍 Searching for pagination links...")
        pagination_patterns = [
            r'href=["\']([^"\']*page=\d+[^"\']*)["\']',
            r'page=(\d+)',
            r'страница\s*(\d+)',
        ]
        
        for pattern in pagination_patterns:
            matches = re.findall(pattern, html_content)
            if matches:
                print(f"   Pattern '{pattern}' found: {matches}")
        
        # Look for specific text that might indicate total count
        print(f"\n🔍 Looking for specific text patterns...")
        if '178 объявлений' in html_content:
            print("   ✅ Found '178 объявлений' in content")
        if 'Найдено 178 объявлений' in html_content:
            print("   ✅ Found 'Найдено 178 объявлений' in content")
        
        # Show a sample of the content around these patterns
        print(f"\n📄 Sample content around results count:")
        lines = html_content.split('\n')
        for i, line in enumerate(lines):
            if '178' in line or 'объявлений' in line or 'Найдено' in line:
                print(f"   Line {i}: {line.strip()}")
        
        # Test the current pagination detection
        print(f"\n🧪 Testing current pagination detection...")
        pagination_info = detect_pagination_info(url)
        print(f"   Pagination info: {pagination_info}")
        
        # Test URL extraction
        print(f"\n🧪 Testing URL extraction...")
        flat_urls = extract_flat_urls_from_search_page(url)
        print(f"   Found {len(flat_urls)} flat URLs")
        
        # Test page URL generation
        print(f"\n🧪 Testing page URL generation...")
        page_urls = generate_page_urls(url, 5)
        print(f"   Generated {len(page_urls)} page URLs:")
        for i, page_url in enumerate(page_urls, 1):
            print(f"     Page {i}: {page_url}")
        
        return html_content
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return None


def test_jazz_pagination():
    """Test pagination for Jazz-квартал."""
    print("\n🧪 Testing Jazz-квартал pagination")
    print("=" * 50)
    
    url = "https://krisha.kz/prodazha/kvartiry/almaty/?das[map.complex]=1206"
    
    # Analyze the page
    analysis = analyze_search_page(url)
    
    print(f"📊 Analysis Results:")
    print(f"   Total flats found: {analysis['total_flats_found']}")
    print(f"   Pagination info: {analysis['pagination_info']}")
    
    # Test pagination detection
    pagination_info = detect_pagination_info(url)
    
    if pagination_info['has_pagination']:
        print(f"\n✅ Pagination detected!")
        print(f"   Max pages: {pagination_info['max_page_found']}")
        print(f"   Total results: {pagination_info['total_results']}")
        
        # Generate URLs for first few pages
        page_urls = generate_page_urls(url, min(3, pagination_info['max_page_found']))
        
        print(f"\n📄 Testing first {len(page_urls)} pages:")
        total_urls = []
        
        for i, page_url in enumerate(page_urls, 1):
            print(f"   Page {i}: {page_url}")
            page_urls = extract_flat_urls_from_search_page(page_url)
            total_urls.extend(page_urls)
            print(f"     Found {len(page_urls)} flats")
        
        # Remove duplicates
        unique_urls = list(set(total_urls))
        print(f"\n✅ Total unique flats across {len(page_urls)} pages: {len(unique_urls)}")
        
    else:
        print(f"\n❌ No pagination detected")


def main():
    """Main debug function."""
    print("🏠 Jazz-квартал Pagination Debug")
    print("=" * 50)
    
    # Debug the page content
    html_content = debug_jazz_page()
    
    # Test pagination
    test_jazz_pagination()
    
    print(f"\n✅ Debug completed!")


if __name__ == "__main__":
    main() 