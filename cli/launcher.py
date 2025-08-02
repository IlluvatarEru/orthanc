#!/usr/bin/env python3
"""
Launcher script for Krisha.kz scraper.

This script can be run with cron to execute scraping jobs every 24 hours.
Example cron job: 0 9 * * * /usr/bin/python3 /path/to/launcher.py
"""

import sys
import os
from datetime import datetime
from scheduler import ScraperScheduler


def main():
    """
    Main launcher function.
    """
    print(f"🚀 Krisha.kz Scraper Launcher")
    print(f"   Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"   Working directory: {os.getcwd()}")
    
    try:
        # Initialize scheduler
        scheduler = ScraperScheduler()
        
        # Run all queries
        print(f"\n📋 Running all configured queries...")
        summary = scheduler.run_all_queries()
        
        # Print summary
        print(f"\n📊 Job Summary:")
        print(f"   Start time: {summary['start_time']}")
        print(f"   End time: {summary['end_time']}")
        print(f"   Duration: {summary['duration_seconds']:.2f} seconds")
        print(f"   Total queries: {summary['total_queries']}")
        print(f"   Rental queries: {summary['rental_queries']}")
        print(f"   Sales queries: {summary['sales_queries']}")
        print(f"   Total flats scraped: {summary['total_flats']}")
        print(f"   Rental flats: {summary['total_rental_flats']}")
        print(f"   Sales flats: {summary['total_sales_flats']}")
        
        # Print detailed results
        print(f"\n📋 Rental Results:")
        for result in summary['rental_results']:
            print(f"   - {result['name']}: {result['scraped_count']} flats")
        
        print(f"\n📋 Sales Results:")
        for result in summary['sales_results']:
            print(f"   - {result['name']}: {result['scraped_count']} flats")
        
        print(f"\n✅ Job completed successfully!")
        return 0
        
    except Exception as e:
        print(f"❌ Error in launcher: {e}")
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code) 