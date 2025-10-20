"""
Example usage of the Krisha.kz scraper.

This script demonstrates how to use the scraper to extract flat information.
"""
from scrapers.src.krisha_scraper import scrape_flat_info
import logging

def main():
    """
    Example usage of the Krisha.kz scraper.
    """
    # Example URLs to test
    test_urls = [
        "https://krisha.kz/a/show/1003924251",
        # Add more URLs here for testing
    ]
    
    logging.info("Krisha.kz Scraper Example")
    logging.info("=" * 50)
    
    for i, url in enumerate(test_urls, 1):
        logging.info(f"\n{i}. Processing: {url}")
        logging.info("-" * 40)
        
        try:
            # Scrape flat information
            flat_info = scrape_flat_info(url)
            
            # Display results
            logging.info(f"Successfully extracted information:")
            logging.info(f"   Flat ID: {flat_info.flat_id}")
            logging.info(f"   💰 Price: {flat_info.price:,} tenge")
            logging.info(f"   📏 Area: {flat_info.area} m²")
            logging.info(f"   🏢 Residential Complex: {flat_info.residential_complex or 'N/A'}")
            logging.info(f"   Floor: {flat_info.floor}/{flat_info.total_floors if flat_info.floor else 'N/A'}")
            logging.info(f"   🏗️ Construction Year: {flat_info.construction_year or 'N/A'}")
            logging.info(f"   🚗 Parking: {flat_info.parking or 'N/A'}")
            logging.info(f"   📝 Description: {flat_info.description[:100]}...")
            
        except Exception as e:
            logging.info(f"Error processing {url}: {e}")
    
    logging.info("\n" + "=" * 50)
    logging.info("Example completed!")


if __name__ == "__main__":
    main() 