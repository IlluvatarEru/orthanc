"""
Test script for the Krisha.kz scraper.
"""
from scrapers.src.krisha_scraper import scrape_flat_info
import logging

def test_scraper():
    """
    Test the scraper with a sample URL.
    """
    # Test URL from the example
    test_url = "https://krisha.kz/a/show/1003924251"
    
    logging.info("Testing Krisha.kz scraper...")
    logging.info(f"URL: {test_url}")
    logging.info("-" * 50)
    
    try:
        # Scrape flat information
        flat_info = scrape_flat_info(test_url)
        
        # Print results
        logging.info("Successfully extracted flat information:")
        logging.info(f"Flat ID: {flat_info.flat_id}")
        logging.info(f"💰 Price: {flat_info.price:,} tenge")
        logging.info(f"📏 Area: {flat_info.area} m²")
        logging.info(f"🏢 Residential Complex: {flat_info.residential_complex or 'N/A'}")
        logging.info(f"Floor: {flat_info.floor}/{flat_info.total_floors if flat_info.floor else 'N/A'}")
        logging.info(f"🏗️ Construction Year: {flat_info.construction_year or 'N/A'}")
        logging.info(f"🚗 Parking: {flat_info.parking or 'N/A'}")
        logging.info(f"📝 Description: {flat_info.description[:100]}...")
        
        # Validate data
        assert flat_info.flat_id == "1003924251"
        assert flat_info.price > 0
        assert flat_info.area > 0
        assert flat_info.description
        
        logging.info("\nAll validations passed!")
        
    except Exception as e:
        logging.info(f"Error during scraping: {e}")
        raise


if __name__ == "__main__":
    test_scraper() 