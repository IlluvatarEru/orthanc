"""
Command-line interface for Krisha.kz search scraper.
"""
from argparse import ArgumentParser

from scrapers.src.search_scraper import analyze_search_page, scrape_search_results, scrape_and_save_search_results
import logging

def main():
    """
    Main CLI function for search scraper.
    """
    parser = ArgumentParser(description='Krisha.kz Search Scraper CLI')
    parser.add_argument('command', choices=['analyze', 'scrape', 'scrape-save'], 
                       help='Command to execute')
    parser.add_argument('url', help='Search page URL')
    parser.add_argument('--max-flats', type=int, help='Maximum number of flats to scrape')
    parser.add_argument('--delay', type=float, default=2.0, help='Delay between requests (seconds)')
    parser.add_argument('--db', default='flats.db', help='Database file path')
    
    args = parser.parse_args()
    
    try:
        if args.command == 'analyze':
            logging.info("Analyzing search page...")
            analysis = analyze_search_page(args.url)
            
            if 'error' in analysis:
                logging.info(f"Analysis failed: {analysis['error']}")
                return
            
            logging.info(f"\nSearch Page Analysis:")
            logging.info(f"URL: {analysis['url']}")
            logging.info(f"Total flats found: {analysis['total_flats_found']}")
            logging.info(f"Total results (if available): {analysis.get('total_results', 'Unknown')}")
            logging.info(f"Current page: {analysis['current_page']}")
            logging.info(f"HTML length: {analysis['html_length']:,} characters")
            
            if analysis['flat_urls']:
                logging.info(f"\nSample flat URLs:")
                for i, url in enumerate(analysis['flat_urls'], 1):
                    logging.info(f"   {i}. {url}")
        
        elif args.command == 'scrape':
            logging.info("Scraping flats from search page...")
            flats = scrape_search_results(
                args.url, 
                max_flats=args.max_flats, 
                delay=args.delay
            )
            
            logging.info(f"\nScraped {len(flats)} flats")
            for flat in flats:
                logging.info(f"   - {flat.flat_id}: {flat.price:,} tenge, {flat.area} m²")
        
        elif args.command == 'scrape-save':
            logging.info("Scraping and saving flats to database...")
            flats = scrape_and_save_search_results(
                args.url,
                db_path=args.db,
                max_flats=args.max_flats,
                delay=args.delay
            )
            
            logging.info(f"\nScraped and saved {len(flats)} flats to database")
            for flat in flats:
                logging.info(f"   - {flat.flat_id}: {flat.price:,} tenge, {flat.area} m²")
    
    except KeyboardInterrupt:
        logging.info("\nOperation cancelled by user")
    except Exception as e:
        logging.info(f"Error: {e}")


if __name__ == "__main__":
    main() 