"""
Enhanced Krisha.kz scraper with database integration.

This module combines scraping functionality with database storage.
"""
import time
from typing import Optional, List
import logging
from common.src.krisha_scraper import FlatInfo, scrape_flat_info
from db.src.database import save_flat_to_db, FlatDatabase


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
        success = save_flat_to_db(flat_info, url, db_path)
        
        if success:
            logging.info(f"Successfully scraped and saved flat {flat_info.flat_id}")
            return flat_info
        else:
            logging.info(f"Failed to save flat {flat_info.flat_id} to database")
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
    db = FlatDatabase(db_path)
    stats = db.get_statistics()
    
    logging.info("\nDatabase Summary:")
    logging.info("=" * 50)
    logging.info(f"Total flats: {stats['total_flats']}")
    logging.info(f"Recent flats (7 days): {stats['recent_flats']}")
    
    if stats['price_stats']['count'] > 0:
        logging.info(f"Price range: {stats['price_stats']['min_price']:,} - {stats['price_stats']['max_price']:,} tenge")
        logging.info(f"Average price: {stats['price_stats']['avg_price']:,.0f} tenge")
    
    if stats['area_stats']['min_area'] is not None:
        logging.info(f"Area range: {stats['area_stats']['min_area']:.1f} - {stats['area_stats']['max_area']:.1f} m²")
        logging.info(f"Average area: {stats['area_stats']['avg_area']:.1f} m²")


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
    db = FlatDatabase(db_path)
    results = db.search_flats(
        min_price=min_price,
        max_price=max_price,
        min_area=min_area,
        max_area=max_area,
        residential_complex=residential_complex,
        limit=limit
    )
    
    logging.info(f"\nSearch Results ({len(results)} flats):")
    logging.info("=" * 50)
    
    for i, flat in enumerate(results, 1):
        logging.info(f"\n{i}. Flat ID: {flat['flat_id']}")
        logging.info(f"   💰 Price: {flat['price']:,} tenge")
        logging.info(f"   📏 Area: {flat['area']} m²")
        logging.info(f"   🏢 Residential Complex: {flat['residential_complex'] or 'N/A'}")
        logging.info(f"   Floor: {flat['floor']}/{flat['total_floors'] if flat['floor'] else 'N/A'}")
        logging.info(f"   🏗️ Construction Year: {flat['construction_year'] or 'N/A'}")
        logging.info(f"   📅 Scraped: {flat['scraped_at']}")


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
    db = FlatDatabase()
    
    # Scrape and save flats
    scraped_flats = scrape_multiple_flats(test_urls, delay=2.0)
    
    # Show database summary
    get_database_summary()
    
    # Example search
    logging.info("\n" + "=" * 50)
    search_flats_in_db(min_price=30000000, max_price=50000000, limit=5)


if __name__ == "__main__":
    main() 