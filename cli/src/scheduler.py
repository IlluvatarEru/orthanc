"""
Scheduler for Krisha.kz scraper.

This module reads configuration from config.toml and runs scraping jobs
for both rental and sales flats with historical tracking.
"""
import time
from datetime import datetime

from db.src.write_read_database import OrthancDB
import toml
import logging

from scrapers.src.search_scraper import scrape_and_save_search_results_with_pagination


class ScraperScheduler:
    """
    Scheduler for running Krisha.kz scraping jobs.
    """
    
    def __init__(self, config_path: str = "config/src/config.toml"):
        """
        Initialize scheduler with configuration.
        
        :param config_path: str, path to configuration file
        """
        self.config_path = config_path
        self.config = self.load_config()
        self.db = OrthancDB(self.config['database']['path'])
        self.setup_logging()
    
    def load_config(self) -> dict:
        """
        Load configuration from TOML file.
        
        :return: dict, configuration data
        """
        try:
            with open(self.config_path, 'rb') as f:
                config = toml.load(f)
            logging.info(f"Loaded configuration from {self.config_path}")
            return config
        except Exception as e:
            logging.info(f"Error loading configuration: {e}")
            raise
    
    def setup_logging(self):
        """
        Setup logging configuration.
        """
        log_config = self.config.get('logging', {})
        log_level = getattr(logging, log_config.get('level', 'INFO'))
        log_file = log_config.get('file', 'scraper.log')
        
        logging.basicConfig(
            level=log_level,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler()
            ]
        )
        
        self.logger = logging.getLogger(__name__)
    
    def scrape_rental_query(self, query_config: dict) -> int:
        """
        Scrape a rental query with pagination and save to database.
        
        :param query_config: dict, rental query configuration
        :return: int, number of flats scraped
        """
        query_name = query_config['name']
        url = query_config['url']
        
        self.logger.info(f"Starting rental query with pagination: {query_name}")
        self.logger.info(f"   URL: {url}")
        
        try:
            # Use paginated scraping
            max_pages = self.config['scraping'].get('max_pages_per_query', 5)
            max_flats = None  # No limit on flats
            delay = self.config['scraping']['delay_between_requests']
            
            # Scrape with pagination
            scraped_flats = scrape_and_save_search_results_with_pagination(
                search_url=url,
                db_path=self.config['database']['path'],
                max_pages=max_pages,
                max_flats=max_flats,
                delay=delay
            )
            
            scraped_count = len(scraped_flats)
            self.logger.info(f"   Completed rental query: {query_name} - {scraped_count} flats scraped")
            return scraped_count
            
        except Exception as e:
            self.logger.error(f"   Error in rental query {query_name}: {e}")
            return 0
    
    def scrape_sales_query(self, query_config: dict) -> int:
        """
        Scrape a sales query with pagination and save to database.
        
        :param query_config: dict, sales query configuration
        :return: int, number of flats scraped
        """
        query_name = query_config['name']
        url = query_config['url']
        
        self.logger.info(f"Starting sales query with pagination: {query_name}")
        self.logger.info(f"   URL: {url}")
        
        try:
            # Use paginated scraping
            max_pages = self.config['scraping'].get('max_pages_per_query', 5)
            max_flats = None  # No limit on flats
            delay = self.config['scraping']['delay_between_requests']
            
            # Scrape with pagination
            scraped_flats = scrape_and_save_search_results_with_pagination(
                search_url=url,
                db_path=self.config['database']['path'],
                max_pages=max_pages,
                max_flats=max_flats,
                delay=delay
            )
            
            scraped_count = len(scraped_flats)
            self.logger.info(f"   Completed sales query: {query_name} - {scraped_count} flats scraped")
            return scraped_count
            
        except Exception as e:
            self.logger.error(f"   Error in sales query {query_name}: {e}")
            return 0
    
    def run_all_queries(self) -> dict:
        """
        Run all configured queries.
        
        :return: dict, summary of results
        """
        self.logger.info("🚀 Starting scheduled scraping job")
        self.logger.info(f"   Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        start_time = time.time()
        
        # Run rental queries
        rental_results = []
        rental_queries = self.config.get('rental_queries', [])
        
        self.logger.info(f"   Found {len(rental_queries)} rental queries")
        
        for query_config in rental_queries:
            scraped_count = self.scrape_rental_query(query_config)
            rental_results.append({
                'name': query_config['name'],
                'scraped_count': scraped_count
            })
        
        # Run sales queries
        sales_results = []
        sales_queries = self.config.get('sales_queries', [])
        
        self.logger.info(f"   Found {len(sales_queries)} sales queries")
        
        for query_config in sales_queries:
            scraped_count = self.scrape_sales_query(query_config)
            sales_results.append({
                'name': query_config['name'],
                'scraped_count': scraped_count
            })
        
        end_time = time.time()
        duration = end_time - start_time
        
        # Calculate summary
        total_rental_flats = sum(r['scraped_count'] for r in rental_results)
        total_sales_flats = sum(r['scraped_count'] for r in sales_results)
        total_flats = total_rental_flats + total_sales_flats
        
        summary = {
            'start_time': datetime.fromtimestamp(start_time).strftime('%Y-%m-%d %H:%M:%S'),
            'end_time': datetime.fromtimestamp(end_time).strftime('%Y-%m-%d %H:%M:%S'),
            'duration_seconds': duration,
            'total_queries': len(rental_queries) + len(sales_queries),
            'rental_queries': len(rental_queries),
            'sales_queries': len(sales_queries),
            'total_flats': total_flats,
            'total_rental_flats': total_rental_flats,
            'total_sales_flats': total_sales_flats,
            'rental_results': rental_results,
            'sales_results': sales_results
        }
        
        self.logger.info("Completed scheduled scraping job")
        self.logger.info(f"   Duration: {duration:.2f} seconds")
        self.logger.info(f"   Total flats scraped: {total_flats}")
        self.logger.info(f"   Rental flats: {total_rental_flats}")
        self.logger.info(f"   Sales flats: {total_sales_flats}")
        
        return summary
    
    def run_single_query(self, query_type: str, query_name: str) -> int:
        """
        Run a single query by name.
        
        :param query_type: str, 'rental' or 'sales'
        :param query_name: str, name of the query to run
        :return: int, number of flats scraped
        """
        queries = self.config.get(f'{query_type}_queries', [])
        
        for query_config in queries:
            if query_config['name'] == query_name:
                if query_type == 'rental':
                    return self.scrape_rental_query(query_config)
                else:
                    return self.scrape_sales_query(query_config)
        
        self.logger.error(f"Query '{query_name}' not found in {query_type} queries")
        return 0


def main():
    """
    Main function to run the scheduler.
    """
    import argparse
    
    parser = argparse.ArgumentParser(description='Krisha.kz Scraper Scheduler')
    parser.add_argument('--config', default='config/config.toml', help='Configuration file path')
    parser.add_argument('--query-type', choices=['rental', 'sales'], help='Run specific query type')
    parser.add_argument('--query-name', help='Run specific query by name')
    parser.add_argument('--all', action='store_true', help='Run all queries')
    
    args = parser.parse_args()
    
    try:
        scheduler = ScraperScheduler(args.config)
        
        if args.query_name and args.query_type:
            # Run single query
            scraped_count = scheduler.run_single_query(args.query_type, args.query_name)
            logging.info(f"Scraped {scraped_count} flats from query '{args.query_name}'")
        
        elif args.all:
            # Run all queries
            summary = scheduler.run_all_queries()
            logging.info(f"\nSummary:")
            logging.info(f"   Total flats scraped: {summary['total_flats']}")
            logging.info(f"   Rental flats: {summary['total_rental_flats']}")
            logging.info(f"   Sales flats: {summary['total_sales_flats']}")
            logging.info(f"   Duration: {summary['duration_seconds']:.2f} seconds")
        
        else:
            # Show available queries
            logging.info("Available queries:")
            logging.info("\nRental queries:")
            for query in scheduler.config.get('rental_queries', []):
                logging.info(f"   - {query['name']}")
            
            logging.info("\nSales queries:")
            for query in scheduler.config.get('sales_queries', []):
                logging.info(f"   - {query['name']}")
            
            logging.info(f"\nUsage:")
            logging.info(f"   python scheduler.py --all")
            logging.info(f"   python scheduler.py --query-type rental --query-name 'Meridian Apartments - 1 room rentals'")
    
    except Exception as e:
        logging.info(f"Error: {e}")


if __name__ == "__main__":
    main() 