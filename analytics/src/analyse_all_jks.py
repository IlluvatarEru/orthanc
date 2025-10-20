#!/usr/bin/env python3
"""
Analyze All JKs Script

This script loops through all residential complexes (JKs) in the database
and retrieves sales and rental data from Krisha.kz for each one.
Data is written to the database immediately after each JK is processed.

Usage from root folder:
python analytics/src/analyse_all_jks.py
"""

import sys
import time
from datetime import datetime
from typing import List, Dict
import logging


# Add the project root to the path
sys.path.insert(0, '.')
from scrapers.src.search_scraper import scrape_complex_data

from db.src.write_read_database import OrthancDB
from analytics.src.jk_analytics import JKAnalytics

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
def get_all_jks_from_db(db_path: str = "flats.db") -> List[Dict]:
    """
    Get all residential complexes from the database.

    :param db_path: str, path to the database file
    :return: List[Dict], list of all residential complexes
    """
    db = OrthancDB(db_path)
    return db.get_all_residential_complexes()


def analyze_single_jk(analytics: JKAnalytics, jk_info: Dict, area_max: float = 100.0) -> Dict:
    """
    Analyze a single residential complex by fetching rental and sales data.

    :param analytics: JKAnalytics, analytics instance
    :param jk_info: Dict, residential complex information
    :param area_max: float, maximum area to consider
    :return: Dict, analysis results
    """
    complex_name = jk_info['name']
    complex_id = jk_info['complex_id']

    logging.info(f" Analyzing JK: {complex_name} (ID: {complex_id})")

    try:
        # Fetch rental data
        logging.info(f" Fetching rental data...")
        rental_success = analytics.fetch_rental_data_if_needed(complex_name, area_max)

        # Fetch sales data
        logging.info(f" Fetching sales data...")
        sales_success = analytics.fetch_sales_data_if_needed(complex_name, area_max)

        # Get analysis results
        # logging.info(f" Getting analysis...")
        # analysis = analytics.get_bucket_analysis(complex_name, area_max)

        # result = {
        #     'complex_name': complex_name,
        #     'complex_id': complex_id,
        #     'rental_success': rental_success,
        #     'sales_success': sales_success,
        #     'total_rental_flats': analysis.get('total_rental_flats', 0),
        #     'total_sales_flats': analysis.get('total_sales_flats', 0),
        #     'overall_stats': analysis.get('overall_stats', {}),
        #     'timestamp': datetime.now().isoformat()
        # }

        # logging.info(f" Completed: {result['total_rental_flats']} rentals, {result['total_sales_flats']} sales")
        #
        # return result
        return {}

    except Exception as e:
        logging.info(f" Error analyzing {complex_name}: {str(e)}")
        return {
            'complex_name': complex_name,
            'complex_id': complex_id,
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }


def analyze_all_jks(db_path: str = "flats.db", area_max: float = 100.0, delay: float = 2.0) -> List[Dict]:
    """
    Analyze all residential complexes in the database.

    :param db_path: str, path to the database file
    :param area_max: float, maximum area to consider for each JK
    :param delay: float, delay between JK analyses in seconds
    :return: List[Dict], results for all JKs
    """
    logging.info(" Starting analysis of all residential complexes...")
    logging.info(f" Database: {db_path}")
    logging.info(f" Area max: {area_max}m²")
    logging.info(f" Delay between JKs: {delay}s")
    logging.info("=" * 80)

    # Get all JKs from database
    jks = get_all_jks_from_db(db_path)
    logging.info(f" Found {len(jks)} residential complexes to analyze")

    if not jks:
        logging.info(" No residential complexes found in database")
        return []

    # Initialize analytics
    analytics = JKAnalytics(db_path)

    # Process each JK
    results = []
    successful = 0
    failed = 0
    for i, jk_info in enumerate(jks, 1):
        logging.info(f"\n[{i}/{len(jks)}] Processing JK...")
        complex_name = jk_info['name']
        complex_id = jk_info['complex_id']
        scrape_complex_data(complex_name, complex_id)

        # # Analyze the JK
        # result = analyze_single_jk(analytics, jk_info, area_max)
        # results.append(result)
        #
        # # Update counters
        # if 'error' in result:
        #     failed += 1
        # else:
        #     successful += 1
        #
        # # Print summary so far
        # logging.info(f" Progress: {successful} successful, {failed} failed")

        # Add delay between requests (except for the last one)
        if i < len(jks):
            logging.info(f" Waiting {delay} seconds before next JK...")
            time.sleep(delay)


    # Print final summary
    logging.info("\n" + "=" * 80)
    logging.info(" ANALYSIS COMPLETE")
    logging.info(f" Successful: {successful}")
    logging.info(f" Failed: {failed}")
    logging.info(f" Total processed: {len(results)}")

    return results


def save_results_to_file(results: List[Dict], filename: str = None) -> str:
    """
    Save analysis results to a JSON file.

    :param results: List[Dict], analysis results
    :param filename: str, optional filename, if None generates timestamped name
    :return: str, filename where results were saved
    """
    import json

    if filename is None:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f"jk_analysis_results_{timestamp}.json"

    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(results, f, indent=2, ensure_ascii=False)

    logging.info(f" Results saved to: {filename}")
    return filename


def main():
    """
    Main function to run the analysis.
    """
    logging.info(" JK Analysis Script")
    logging.info("=" * 50)

    # Configuration
    db_path = "flats.db"
    area_max = 100.0  # Maximum area to consider
    delay = 2.0  # Delay between JK analyses

    try:
        # Run the analysis
        results = analyze_all_jks(db_path, area_max, delay)

        logging.info("\n Analysis completed successfully!")

    except KeyboardInterrupt:
        logging.info("\n Analysis interrupted by user")
        sys.exit(1)
    except Exception as e:
        logging.info(f"\n Error during analysis: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
