
"""
Debug script to find missing flats.
"""
from scrapers.src.search_scraper import detect_pagination_info, generate_page_urls, extract_flat_urls_from_search_page


def debug_missing_flats():
    """Debug why we're missing flats."""
    print("🔍 Debugging Missing Flats")
    print("=" * 50)
    
    url = "https://krisha.kz/prodazha/kvartiry/almaty/?das[map.complex]=1206"
    
    # Get pagination info
    pagination_info = detect_pagination_info(url)
    print(f"📊 Krisha shows: {pagination_info['total_results']} total results")
    print(f"📄 Max pages: {pagination_info['max_page_found']}")
    print(f"📄 Estimated pages: {pagination_info['estimated_pages']}")
    
    # Generate all page URLs
    page_urls = generate_page_urls(url, pagination_info['max_page_found'])
    print(f"\n📄 Will scrape {len(page_urls)} pages")
    
    all_flat_urls = []
    page_stats = []
    
    # Check each page individually
    for i, page_url in enumerate(page_urls, 1):
        print(f"\n📄 Page {i}: {page_url}")
        
        try:
            # Extract URLs from this page
            page_flat_urls = extract_flat_urls_from_search_page(page_url)
            
            # Convert to flat IDs for easier tracking
            flat_ids = [url.split('/')[-1] for url in page_flat_urls]
            
            page_stats.append({
                'page': i,
                'urls_found': len(page_flat_urls),
                'flat_ids': flat_ids
            })
            
            all_flat_urls.extend(page_flat_urls)
            
            print(f"   ✅ Found {len(page_flat_urls)} flats")
            print(f"   Sample flat IDs: {flat_ids[:5]}")
            
        except Exception as e:
            print(f"   ❌ Error on page {i}: {e}")
            page_stats.append({
                'page': i,
                'urls_found': 0,
                'flat_ids': [],
                'error': str(e)
            })
    
    # Remove duplicates
    unique_urls = list(set(all_flat_urls))
    unique_flat_ids = [url.split('/')[-1] for url in unique_urls]
    
    print(f"\n📊 Summary:")
    print(f"   Total URLs found: {len(all_flat_urls)}")
    print(f"   Unique URLs: {len(unique_urls)}")
    print(f"   Duplicates removed: {len(all_flat_urls) - len(unique_urls)}")
    print(f"   Expected from Krisha: {pagination_info['total_results']}")
    print(f"   Missing: {pagination_info['total_results'] - len(unique_urls)}")
    
    # Check for missing pages
    print(f"\n📄 Page-by-page breakdown:")
    for stat in page_stats:
        if 'error' in stat:
            print(f"   Page {stat['page']}: ❌ ERROR - {stat['error']}")
        else:
            print(f"   Page {stat['page']}: {stat['urls_found']} flats")
    
    # Check if we're missing the last page
    if len(page_urls) < pagination_info['max_page_found']:
        print(f"\n⚠️  WARNING: We're only scraping {len(page_urls)} pages but Krisha has {pagination_info['max_page_found']} pages!")
    
    # Check if some pages have fewer than 20 flats
    for stat in page_stats:
        if 'error' not in stat and stat['urls_found'] < 20:
            print(f"\n⚠️  WARNING: Page {stat['page']} only has {stat['urls_found']} flats (expected 20)")
    
    return unique_flat_ids, pagination_info['total_results']


def check_database_flats():
    """Check what's actually in our database."""
    print(f"\n🗄️  Database Check")
    print("=" * 50)
    
    import sqlite3
    conn = sqlite3.connect('flats.db')
    cursor = conn.cursor()
    
    # Get all Jazz flats
    cursor.execute("""
        SELECT flat_id, residential_complex, area, price 
        FROM sales_flats 
        WHERE residential_complex LIKE '%Jazz%'
        ORDER BY flat_id
    """)
    
    db_flats = cursor.fetchall()
    db_flat_ids = [row[0] for row in db_flats]
    
    print(f"📊 Database contains {len(db_flat_ids)} Jazz flats")
    print(f"   Sample flat IDs: {db_flat_ids[:10]}")
    
    # Check for duplicates in database
    unique_db_ids = list(set(db_flat_ids))
    print(f"   Unique flat IDs in DB: {len(unique_db_ids)}")
    print(f"   Duplicates in DB: {len(db_flat_ids) - len(unique_db_ids)}")
    
    conn.close()
    return unique_db_ids


def main():
    """Main debug function."""
    print("🏠 Missing Flats Debug")
    print("=" * 50)
    
    # Check what we can scrape
    scraped_ids, krisha_total = debug_missing_flats()
    
    # Check what's in database
    db_ids = check_database_flats()
    
    # Compare
    print(f"\n🔍 Comparison:")
    print(f"   Krisha total: {krisha_total}")
    print(f"   Scraped unique: {len(scraped_ids)}")
    print(f"   In database: {len(db_ids)}")
    print(f"   Missing from scraping: {krisha_total - len(scraped_ids)}")
    print(f"   Missing from database: {krisha_total - len(db_ids)}")
    
    # Check if database has flats that weren't scraped
    scraped_set = set(scraped_ids)
    db_set = set(db_ids)
    
    in_db_not_scraped = db_set - scraped_set
    in_scraped_not_db = scraped_set - db_set
    
    if in_db_not_scraped:
        print(f"\n⚠️  Flats in DB but not in current scraping: {len(in_db_not_scraped)}")
        print(f"   Sample: {list(in_db_not_scraped)[:5]}")
    
    if in_scraped_not_db:
        print(f"\n⚠️  Flats scraped but not in DB: {len(in_scraped_not_db)}")
        print(f"   Sample: {list(in_scraped_not_db)[:5]}")


if __name__ == "__main__":
    main() 