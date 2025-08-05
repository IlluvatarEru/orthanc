#!/usr/bin/env python3
"""
Debug script to test specific missing flats.
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from common.krisha_scraper import scrape_flat_info
from db.enhanced_database import save_sales_flat_to_db
from datetime import datetime

def test_missing_flats():
    """Test scraping the specific missing flats."""
    print("🔍 Testing Missing Flats")
    print("=" * 50)
    
    # These are the flats that were scraped but not saved to DB
    missing_flat_ids = [
        '694201731', '761497976', '1003901463', '697902551', '1003925052',
        '1003923737', '1003456782', '1003925103', '1003925385', '1003925430',
        '1002860846', '1002745533', '1002884477', '1003935945', '1003939188',
        '1003940239', '1003940973', '1003942284'
    ]
    
    print(f"📊 Testing {len(missing_flat_ids)} missing flats")
    
    success_count = 0
    error_count = 0
    
    for i, flat_id in enumerate(missing_flat_ids, 1):
        url = f"https://krisha.kz/a/show/{flat_id}"
        print(f"\n[{i}/{len(missing_flat_ids)}] Testing: {url}")
        
        try:
            # Try to scrape the flat
            flat_info = scrape_flat_info(url)
            print(f"   ✅ Successfully scraped flat {flat_id}")
            print(f"   Price: {flat_info.price:,} tenge")
            print(f"   Area: {flat_info.area} m²")
            print(f"   Complex: {flat_info.residential_complex}")
            
            # Try to save to database
            query_date = datetime.now().strftime('%Y-%m-%d')
            success = save_sales_flat_to_db(flat_info, url, query_date)
            
            if success:
                print(f"   ✅ Successfully saved to database")
                success_count += 1
            else:
                print(f"   ❌ Failed to save to database")
                error_count += 1
                
        except Exception as e:
            print(f"   ❌ Error scraping {flat_id}: {e}")
            error_count += 1
    
    print(f"\n📊 Results:")
    print(f"   Successfully scraped and saved: {success_count}")
    print(f"   Failed: {error_count}")
    print(f"   Success rate: {success_count/(success_count+error_count)*100:.1f}%")


def check_database_after_test():
    """Check database after testing missing flats."""
    print(f"\n🗄️  Database Check After Test")
    print("=" * 50)
    
    import sqlite3
    conn = sqlite3.connect('flats.db')
    cursor = conn.cursor()
    
    # Get all Jazz flats
    cursor.execute("""
        SELECT COUNT(*) FROM sales_flats 
        WHERE residential_complex LIKE '%Jazz%'
    """)
    
    total_flats = cursor.fetchone()[0]
    
    cursor.execute("""
        SELECT COUNT(DISTINCT flat_id) FROM sales_flats 
        WHERE residential_complex LIKE '%Jazz%'
    """)
    
    unique_flats = cursor.fetchone()[0]
    
    print(f"📊 Database after test:")
    print(f"   Total records: {total_flats}")
    print(f"   Unique flats: {unique_flats}")
    print(f"   Duplicates: {total_flats - unique_flats}")
    
    conn.close()


def main():
    """Main debug function."""
    print("🏠 Missing Flats Specific Test")
    print("=" * 50)
    
    # Test the missing flats
    test_missing_flats()
    
    # Check database after test
    check_database_after_test()


if __name__ == "__main__":
    main() 