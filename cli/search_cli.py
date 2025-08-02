"""
Command-line interface for Krisha.kz search scraper.
"""

import argparse
import sys
from scrapers.search_scraper import analyze_search_page, scrape_search_results, scrape_and_save_search_results


def main():
    """
    Main CLI function for search scraper.
    """
    parser = argparse.ArgumentParser(description='Krisha.kz Search Scraper CLI')
    parser.add_argument('command', choices=['analyze', 'scrape', 'scrape-save'], 
                       help='Command to execute')
    parser.add_argument('url', help='Search page URL')
    parser.add_argument('--max-flats', type=int, help='Maximum number of flats to scrape')
    parser.add_argument('--delay', type=float, default=2.0, help='Delay between requests (seconds)')
    parser.add_argument('--db', default='flats.db', help='Database file path')
    
    args = parser.parse_args()
    
    try:
        if args.command == 'analyze':
            print("🔍 Analyzing search page...")
            analysis = analyze_search_page(args.url)
            
            if 'error' in analysis:
                print(f"❌ Analysis failed: {analysis['error']}")
                return
            
            print(f"\n📊 Search Page Analysis:")
            print(f"URL: {analysis['url']}")
            print(f"Total flats found: {analysis['total_flats_found']}")
            print(f"Total results (if available): {analysis.get('total_results', 'Unknown')}")
            print(f"Current page: {analysis['current_page']}")
            print(f"HTML length: {analysis['html_length']:,} characters")
            
            if analysis['flat_urls']:
                print(f"\n🔗 Sample flat URLs:")
                for i, url in enumerate(analysis['flat_urls'], 1):
                    print(f"   {i}. {url}")
        
        elif args.command == 'scrape':
            print("🏠 Scraping flats from search page...")
            flats = scrape_search_results(
                args.url, 
                max_flats=args.max_flats, 
                delay=args.delay
            )
            
            print(f"\n✅ Scraped {len(flats)} flats")
            for flat in flats:
                print(f"   - {flat.flat_id}: {flat.price:,} tenge, {flat.area} m²")
        
        elif args.command == 'scrape-save':
            print("🏠 Scraping and saving flats to database...")
            flats = scrape_and_save_search_results(
                args.url,
                db_path=args.db,
                max_flats=args.max_flats,
                delay=args.delay
            )
            
            print(f"\n✅ Scraped and saved {len(flats)} flats to database")
            for flat in flats:
                print(f"   - {flat.flat_id}: {flat.price:,} tenge, {flat.area} m²")
    
    except KeyboardInterrupt:
        print("\n⚠️ Operation cancelled by user")
    except Exception as e:
        print(f"❌ Error: {e}")


if __name__ == "__main__":
    main() 