"""
Enhanced Krisha.kz scraper with database integration.

This module combines scraping functionality with database storage.
"""
import time
from typing import Optional, List
import logging
from common.src.flat_info import FlatInfo
from .krisha_scraper import scrape_flat_info
from db.src.write_read_database import OrthancDB


def scrape_and_save(url: str, db_path: str = "flats.db") -> Optional[FlatInfo]:
    """
    Scrape flat information and save to database.
    
    :param url: str, URL of the flat to scrape
    :param db_path: str, database file path
    :return: Optional[FlatInfo], scraped flat information or None if failed
    """
    try:
        # Scrape flat information
        flat_info = scrape_flat_info(url)
        
        # Save to database
        db = OrthancDB(db_path)
        # Determine if it's rental or sales based on URL or other criteria
        # For now, we'll assume it's sales if not explicitly rental
        if hasattr(flat_info, 'is_rental') and flat_info.is_rental:
            success = db.insert_rental_flat(flat_info, url, "2024-01-01")  # Default date
        else:
            success = db.insert_sales_flat(flat_info, url, "2024-01-01")  # Default date
        
        if success:
            logging.info(f"Successfully scraped and saved flat {flat_info.flat_id}")
            return flat_info
        else:
            logging.error(f"Failed to save flat {flat_info.flat_id} to database")
            return flat_info
            
    except Exception as e:
        logging.info(f"Error scraping {url}: {e}")
        return None


def scrape_multiple_flats(urls: List[str], db_path: str = "flats.db", delay: float = 1.0) -> List[FlatInfo]:
    """
    Scrape multiple flats with delay between requests.
    
    :param urls: List[str], list of URLs to scrape
    :param db_path: str, database file path
    :param delay: float, delay between requests in seconds
    :return: List[FlatInfo], list of successfully scraped flats
    """
    results = []
    
    logging.info(f"Starting to scrape {len(urls)} flats...")
    
    for i, url in enumerate(urls, 1):
        logging.info(f"\n[{i}/{len(urls)}] Processing: {url}")
        
        flat_info = scrape_and_save(url, db_path)
        if flat_info:
            results.append(flat_info)
        
        # Add delay between requests to be respectful
        if i < len(urls):
            time.sleep(delay)
    
    logging.info(f"\nCompleted! Successfully scraped {len(results)}/{len(urls)} flats")
    return results


def get_database_summary(db_path: str = "flats.db") -> None:
    """
    Display database summary.
    
    :param db_path: str, database file path
    """
    db = OrthancDB(db_path)
    
    # Get counts for each type
    rental_count = db.get_flat_count('rental')
    sales_count = db.get_flat_count('sales')
    complex_count = db.get_complex_count()
    
    logging.info("\nDatabase Summary:")
    logging.info("=" * 50)
    logging.info(f"Total rental flats: {rental_count}")
    logging.info(f"Total sales flats: {sales_count}")
    logging.info(f"Total residential complexes: {complex_count}")
    
    # Get historical stats for the last 30 days
    from datetime import datetime, timedelta
    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
    
    try:
        stats = db.get_historical_statistics(start_date, end_date)
        
        if stats['rental_stats']['total_rentals'] > 0:
            logging.info(f"Rental price range: {stats['rental_stats']['min_rental_price']:,} - {stats['rental_stats']['max_rental_price']:,} tenge")
            logging.info(f"Average rental price: {stats['rental_stats']['avg_rental_price']:,.0f} tenge")
        
        if stats['sales_stats']['total_sales'] > 0:
            logging.info(f"Sales price range: {stats['sales_stats']['min_sales_price']:,} - {stats['sales_stats']['max_sales_price']:,} tenge")
            logging.info(f"Average sales price: {stats['sales_stats']['avg_sales_price']:,.0f} tenge")
    except Exception as e:
        logging.info(f"Could not retrieve historical statistics: {e}")


def search_flats_in_db(min_price: Optional[int] = None,
                      max_price: Optional[int] = None,
                      min_area: Optional[float] = None,
                      max_area: Optional[float] = None,
                      residential_complex: Optional[str] = None,
                      limit: Optional[int] = None,
                      db_path: str = "flats.db") -> None:
    """
    Search and display flats from database.
    
    :param min_price: Optional[int], minimum price filter
    :param max_price: Optional[int], maximum price filter
    :param min_area: Optional[float], minimum area filter
    :param max_area: Optional[float], maximum area filter
    :param residential_complex: Optional[str], residential complex filter
    :param limit: Optional[int], maximum number of results
    :param db_path: str, database file path
    """
    db = OrthancDB(db_path)
    
    # Get recent flats from both rental and sales tables
    from datetime import datetime
    today = datetime.now().strftime('%Y-%m-%d')
    
    rental_results = db.get_rental_flats_by_date(today, limit)
    sales_results = db.get_sales_flats_by_date(today, limit)
    
    # Combine and filter results
    all_results = []
    
    for flat in rental_results:
        if _matches_filters(flat, min_price, max_price, min_area, max_area, residential_complex):
            flat['type'] = 'rental'
            all_results.append(flat)
    
    for flat in sales_results:
        if _matches_filters(flat, min_price, max_price, min_area, max_area, residential_complex):
            flat['type'] = 'sales'
            all_results.append(flat)
    
    # Sort by price
    all_results.sort(key=lambda x: x['price'])
    
    # Apply limit
    if limit:
        all_results = all_results[:limit]
    
    logging.info(f"\nSearch Results ({len(all_results)} flats):")
    logging.info("=" * 50)
    
    for i, flat in enumerate(all_results, 1):
        flat_type = flat.get('type', 'unknown')
        logging.info(f"\n{i}. Flat ID: {flat['flat_id']} ({flat_type})")
        logging.info(f"   💰 Price: {flat['price']:,} tenge")
        logging.info(f"   📏 Area: {flat['area']} m²")
        logging.info(f"   🏢 Residential Complex: {flat['residential_complex'] or 'N/A'}")
        logging.info(f"   Floor: {flat['floor']}/{flat['total_floors'] if flat['floor'] else 'N/A'}")
        logging.info(f"   🏗️ Construction Year: {flat['construction_year'] or 'N/A'}")
        logging.info(f"   📅 Scraped: {flat['scraped_at']}")


def _matches_filters(flat: dict, min_price: Optional[int], max_price: Optional[int], 
                    min_area: Optional[float], max_area: Optional[float], 
                    residential_complex: Optional[str]) -> bool:
    """
    Check if a flat matches the given filters.
    """
    if min_price is not None and flat['price'] < min_price:
        return False
    if max_price is not None and flat['price'] > max_price:
        return False
    if min_area is not None and flat['area'] < min_area:
        return False
    if max_area is not None and flat['area'] > max_area:
        return False
    if residential_complex and residential_complex.lower() not in (flat['residential_complex'] or '').lower():
        return False
    return True


def main():
    """
    Example usage of the enhanced scraper with database.
    """
    # Example URLs to scrape
    test_urls = [
        "https://krisha.kz/a/show/1003924251",
        # Add more URLs here
    ]
    
    logging.info("Krisha.kz Scraper with Database")
    logging.info("=" * 50)
    
    # Initialize database
    db = OrthancDB()
    
    # Scrape and save flats
    scraped_flats = scrape_multiple_flats(test_urls, delay=2.0)
    
    # Show database summary
    get_database_summary()
    
    # Example search
    logging.info("\n" + "=" * 50)
    search_flats_in_db(min_price=30000000, max_price=50000000, limit=5)


if __name__ == "__main__":
    main() 