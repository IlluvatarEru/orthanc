"""
Database operations for retrieving flat information.

This module provides functionality to get flat data from database or scrape from web.
"""

from datetime import datetime
from typing import Optional

from flask import flash

from common.src.flat_info import FlatInfo
from common.src.krisha_scraper import scrape_flat_info_with_type
from .write_read_database import FlatDatabase, save_sales_flat_to_db
import logging


def get_flat_info(flat_id: str, flash_to_frontend: bool = True, db_path: str = "flats.db") -> Optional[FlatInfo]:
    """
    Get flat information from database or scrape from web.

    :param flat_id: str, flat ID
    :param flash_to_frontend: bool, whether to flash messages to frontend
    :param db_path: str, database file path
    :return: Optional[FlatInfo], FlatInfo object or None if error
    """
    logging.info(f"flat_id = -{flat_id}- of type {type(flat_id)}")
    db = FlatDatabase(db_path)
    rental_count = db.get_flat_count('rental')
    logging.info(f"rental_count = {rental_count}")
    try:
        db.connect()

        # Check if flat exists in database
        q = f"""
            SELECT flat_id, price, area, flat_type, residential_complex, floor, total_floors, 
                   construction_year, parking, description, url, query_date
            FROM sales_flats 
            WHERE flat_id = {flat_id}
            ORDER BY query_date DESC 
            LIMIT 1
        """
        logging.info(q)
        cursor = db.conn.execute(q)

        existing_flat = cursor.fetchone()
        logging.info(existing_flat)

        if existing_flat:
            logging.info(f"Using existing flat data from database for {flat_id}")
            return FlatInfo(
                flat_id=existing_flat[0],
                price=existing_flat[1],
                area=existing_flat[2],
                flat_type=existing_flat[3],
                residential_complex=existing_flat[4],
                floor=existing_flat[5],
                total_floors=existing_flat[6],
                construction_year=existing_flat[7],
                parking=existing_flat[8],
                description=existing_flat[9],
                is_rental=False
            )
        else:
            # Scrape fresh data from web
            logging.info(f"🌐 Scraping fresh flat data for {flat_id}")
            flat_url = f"https://krisha.kz/a/show/{flat_id}"

            try:
                flat_info = scrape_flat_info_with_type(flat_url)

                # Check if flat is for rent
                if flat_info.is_rental:
                    if flash_to_frontend:
                        flash(f'Error: Flat ID {flat_id} is for rent (Аренда). Please provide the ID of a flat for sale.',
                              'error')
                    return None

                # Save to database
                query_date = datetime.now().strftime('%Y-%m-%d')
                save_sales_flat_to_db(flat_info, flat_url, query_date, flat_info.flat_type)
                return flat_info

            except Exception as e:
                logging.info(f"Error scraping flat {flat_id}: {e}")
                if flash_to_frontend:
                    flash(f'Error: Could not scrape flat {flat_id}. Please check if the flat ID is correct.', 'error')
                return None

    finally:
        db.disconnect()
