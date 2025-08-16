
"""
Launcher script for Krisha.kz scraper.

This script can be run with cron to execute scraping jobs every 24 hours.
Example cron job: 0 9 * * * /usr/bin/python3 /path/to/launcher.py
"""
import sys
from datetime import datetime
from os import getcwd

from cli.src.scheduler import ScraperScheduler

import logging
def main():
    """
    Main launcher function.
    """
    logging.info(f"🚀 Krisha.kz Scraper Launcher")
    logging.info(f"   Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logging.info(f"   Working directory: {getcwd()}")
    
    try:
        # Initialize scheduler
        scheduler = ScraperScheduler()
        
        # Run all queries
        logging.info(f"\nRunning all configured queries...")
        summary = scheduler.run_all_queries()
        
        # Print summary
        logging.info(f"\nJob Summary:")
        logging.info(f"   Start time: {summary['start_time']}")
        logging.info(f"   End time: {summary['end_time']}")
        logging.info(f"   Duration: {summary['duration_seconds']:.2f} seconds")
        logging.info(f"   Total queries: {summary['total_queries']}")
        logging.info(f"   Rental queries: {summary['rental_queries']}")
        logging.info(f"   Sales queries: {summary['sales_queries']}")
        logging.info(f"   Total flats scraped: {summary['total_flats']}")
        logging.info(f"   Rental flats: {summary['total_rental_flats']}")
        logging.info(f"   Sales flats: {summary['total_sales_flats']}")
        
        # Print detailed results
        logging.info(f"\nRental Results:")
        for result in summary['rental_results']:
            logging.info(f"   - {result['name']}: {result['scraped_count']} flats")
        
        logging.info(f"\nSales Results:")
        for result in summary['sales_results']:
            logging.info(f"   - {result['name']}: {result['scraped_count']} flats")
        
        logging.info(f"\nJob completed successfully!")
        return 0
        
    except Exception as e:
        logging.info(f"Error in launcher: {e}")
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code) 