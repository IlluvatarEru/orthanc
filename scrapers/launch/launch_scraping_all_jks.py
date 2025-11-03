"""
Launch script for scraping all JK rentals and sales.

This module provides functions to scrape all residential complexes (JKs)
from the database, both for rentals and sales, with daily scheduling options.

python -m scrapers.launch.launch_scraping_all_jks
"""

import logging
import sys
import time
from datetime import datetime, timedelta
from typing import List, Dict

from db.src.write_read_database import OrthancDB
from scrapers.src.krisha_rental_scraping import scrape_and_save_jk_rentals
from scrapers.src.krisha_sales_scraping import scrape_and_save_jk_sales
from scrapers.src.residential_complex_scraper import update_complex_database

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('jk_scraping.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def get_all_jks_from_db(db_path: str = "flats.db") -> List[Dict]:
    """
    Get all residential complexes (JKs) from the residential_complexes table, excluding blacklisted ones.
    
    :param db_path: str, path to database file
    :return: List[Dict], list of JK information
    """
    logger.info("Fetching all JKs from residential_complexes table (excluding blacklisted)")

    db = OrthancDB(db_path)
    db.connect()
    try:
        # Get JKs from residential_complexes table, excluding blacklisted ones
        cursor = db.conn.execute("""
            SELECT name as residential_complex, complex_id, city, district
            FROM residential_complexes 
            WHERE name NOT IN (
                SELECT name FROM blacklisted_jks
            )
            ORDER BY name
        """)

        jks = [dict(row) for row in cursor.fetchall()]
        logger.info(f"Found {len(jks)} JKs in residential_complexes table (excluding blacklisted)")

        # Log blacklisted JKs for transparency
        blacklisted_jks = db.get_blacklisted_jks()
        if blacklisted_jks:
            logger.info(f"Excluded {len(blacklisted_jks)} blacklisted JKs: {[jk['name'] for jk in blacklisted_jks]}")

        return jks

    except Exception as e:
        logger.error(f"Error fetching JKs from database: {e}")
        return []
    finally:
        db.disconnect()


def scrape_all_jk_rentals(db_path: str = "flats.db", max_pages: int = 1) -> Dict[str, int]:
    """
    Scrape all JK rentals from the database.
    
    :param db_path: str, path to database file
    :param max_pages: int, maximum pages to scrape per JK
    :return: Dict[str, int], results with JK names and saved counts
    """
    logger.info("Starting scraping of all JK rentals")

    jks = get_all_jks_from_db(db_path)
    if not jks:
        logger.warning("No JKs found in database")
        return {}

    results = {}
    total_saved = 0

    for i, jk_info in enumerate(jks, 1):
        jk_name = jk_info['residential_complex']
        logger.info(f"[{i}/{len(jks)}] Scraping rentals for: {jk_name}")

        try:
            saved_count = scrape_and_save_jk_rentals(
                jk_name=jk_name,
                max_pages=max_pages,
                db_path=db_path
            )

            results[jk_name] = saved_count
            total_saved += saved_count

            logger.info(f"✅ {jk_name}: {saved_count} rental flats saved")

            # Add delay between JKs to be respectful
            time.sleep(2)

        except Exception as e:
            logger.error(f"❌ Error scraping {jk_name}: {e}")
            results[jk_name] = 0

    logger.info(f"Completed JK rental scraping. Total saved: {total_saved}")
    return results


def scrape_all_jk_sales(db_path: str = "flats.db", max_pages: int = 1) -> Dict[str, int]:
    """
    Scrape all JK sales from the database.
    
    :param db_path: str, path to database file
    :param max_pages: int, maximum pages to scrape per JK
    :return: Dict[str, int], results with JK names and saved counts
    """
    logger.info("Starting scraping of all JK sales")

    jks = get_all_jks_from_db(db_path)
    if not jks:
        logger.warning("No JKs found in database")
        return {}

    results = {}
    total_saved = 0

    for i, jk_info in enumerate(jks, 1):
        jk_name = jk_info['residential_complex']
        logger.info(f"[{i}/{len(jks)}] Scraping sales for: {jk_name}")

        try:
            saved_count = scrape_and_save_jk_sales(
                jk_name=jk_name,
                max_pages=max_pages,
                db_path=db_path
            )

            results[jk_name] = saved_count
            total_saved += saved_count

            logger.info(f"✅ {jk_name}: {saved_count} sales flats saved")

            # Add delay between JKs to be respectful
            time.sleep(2)

        except Exception as e:
            logger.error(f"❌ Error scraping {jk_name}: {e}")
            results[jk_name] = 0

    logger.info(f"Completed JK sales scraping. Total saved: {total_saved}")
    return results


def daily_rental_scraping_loop(db_path: str = "flats.db", max_pages: int = 1,
                               run_time: str = "12:00") -> None:
    """
    Run daily rental scraping in a continuous loop.
    Starts immediately when launched, then runs at the specified time each day.
    
    :param db_path: str, path to database file
    :param max_pages: int, maximum pages to scrape per JK
    :param run_time: str, time to run scraping (format: "HH:MM")
    """
    logger.info(f"Starting daily rental scraping loop (immediate run, then daily at {run_time})")

    # Run immediately on startup
    logger.info("Running initial rental scraping immediately...")
    try:
        results = scrape_all_jk_rentals(db_path, max_pages)

        # Log summary
        total_saved = sum(results.values())
        successful_jks = len([r for r in results.values() if r > 0])

        logger.info(f"Initial rental scraping completed:")
        logger.info(f"  - JKs processed: {len(results)}")
        logger.info(f"  - Successful JKs: {successful_jks}")
        logger.info(f"  - Total flats saved: {total_saved}")
    except Exception as e:
        logger.error(f"Error in initial rental scraping: {e}")

    # Parse target time
    target_hour, target_minute = map(int, run_time.split(':'))

    # Calculate next run time
    now = datetime.now()
    next_run = now.replace(hour=target_hour, minute=target_minute, second=0, microsecond=0)
    if next_run <= now:
        next_run = next_run + timedelta(days=1)

    logging.info(f"Next launch at {next_run.strftime('%Y-%m-%d %H:%M:%S')}")

    while True:
        try:
            now = datetime.now()
            current_time = now.strftime("%H:%M")

            # Check if it's time to run (within 1 minute of target time)
            if (now.hour == target_hour and now.minute == target_minute) or \
                    (now.hour == target_hour and now.minute == target_minute + 1):
                logger.info(f"Starting daily rental scraping at {current_time}")

                results = scrape_all_jk_rentals(db_path, max_pages)

                # Log summary
                total_saved = sum(results.values())
                successful_jks = len([r for r in results.values() if r > 0])

                logger.info(f"Daily rental scraping completed:")
                logger.info(f"  - JKs processed: {len(results)}")
                logger.info(f"  - Successful JKs: {successful_jks}")
                logger.info(f"  - Total flats saved: {total_saved}")

                # Wait until next day to avoid multiple runs
                next_run = now.replace(hour=23, minute=59, second=59)
                wait_seconds = (next_run - now).total_seconds()
                logger.info(f"Waiting {wait_seconds / 3600:.1f} hours until next run")

                # Calculate and log the next scheduled run time
                tomorrow_run = (now + timedelta(days=1)).replace(hour=target_hour, minute=target_minute, second=0,
                                                                 microsecond=0)
                logger.info(f"Next launch at {tomorrow_run.strftime('%Y-%m-%d %H:%M:%S')}")

                time.sleep(wait_seconds)
            else:
                # Check every minute
                time.sleep(60)

        except KeyboardInterrupt:
            logger.info("Daily rental scraping loop stopped by user")
            break
        except Exception as e:
            logger.error(f"Error in daily rental scraping loop: {e}")
            time.sleep(300)  # Wait 5 minutes before retrying


def daily_sales_scraping_loop(db_path: str = "flats.db", max_pages: int = 1,
                              run_time: str = "13:00") -> None:
    """
    Run daily sales scraping in a continuous loop.
    Starts immediately when launched, then runs at the specified time each day.
    
    :param db_path: str, path to database file
    :param max_pages: int, maximum pages to scrape per JK
    :param run_time: str, time to run scraping (format: "HH:MM")
    """
    logger.info(f"Starting daily sales scraping loop (immediate run, then daily at {run_time})")

    # Run immediately on startup
    logger.info("Running initial sales scraping immediately...")
    try:
        results = scrape_all_jk_sales(db_path, max_pages)

        # Log summary
        total_saved = sum(results.values())
        successful_jks = len([r for r in results.values() if r > 0])

        logger.info(f"Initial sales scraping completed:")
        logger.info(f"  - JKs processed: {len(results)}")
        logger.info(f"  - Successful JKs: {successful_jks}")
        logger.info(f"  - Total flats saved: {total_saved}")
    except Exception as e:
        logger.error(f"Error in initial sales scraping: {e}")

    # Parse target time
    target_hour, target_minute = map(int, run_time.split(':'))

    # Calculate next run time
    now = datetime.now()
    next_run = now.replace(hour=target_hour, minute=target_minute, second=0, microsecond=0)
    if next_run <= now:
        next_run = next_run + timedelta(days=1)

    logging.info(f"Next launch at {next_run.strftime('%Y-%m-%d %H:%M:%S')}")

    while True:
        try:
            now = datetime.now()
            current_time = now.strftime("%H:%M")

            # Check if it's time to run (within 1 minute of target time)
            if (now.hour == target_hour and now.minute == target_minute) or \
                    (now.hour == target_hour and now.minute == target_minute + 1):
                logger.info(f"Starting daily sales scraping at {current_time}")

                results = scrape_all_jk_sales(db_path, max_pages)

                # Log summary
                total_saved = sum(results.values())
                successful_jks = len([r for r in results.values() if r > 0])

                logger.info(f"Daily sales scraping completed:")
                logger.info(f"  - JKs processed: {len(results)}")
                logger.info(f"  - Successful JKs: {successful_jks}")
                logger.info(f"  - Total flats saved: {total_saved}")

                # Wait until next day to avoid multiple runs
                next_run = now.replace(hour=23, minute=59, second=59)
                wait_seconds = (next_run - now).total_seconds()
                logger.info(f"Waiting {wait_seconds / 3600:.1f} hours until next run")

                # Calculate and log the next scheduled run time
                tomorrow_run = (now + timedelta(days=1)).replace(hour=target_hour, minute=target_minute, second=0,
                                                                 microsecond=0)
                logger.info(f"Next launch at {tomorrow_run.strftime('%Y-%m-%d %H:%M:%S')}")

                time.sleep(wait_seconds)
            else:
                # Check every minute
                time.sleep(60)

        except KeyboardInterrupt:
            logger.info("Daily sales scraping loop stopped by user")
            break
        except Exception as e:
            logger.error(f"Error in daily sales scraping loop: {e}")
            time.sleep(300)  # Wait 5 minutes before retrying


def run_immediate_scraping(db_path: str = "flats.db", max_pages: int = 1,
                           scrape_rentals: bool = True, scrape_sales: bool = True) -> None:
    """
    Run immediate scraping of all JKs (rentals and/or sales).
    
    :param db_path: str, path to database file
    :param max_pages: int, maximum pages to scrape per JK
    :param scrape_rentals: bool, whether to scrape rentals
    :param scrape_sales: bool, whether to scrape sales
    """
    logger.info("Starting immediate scraping of all JKs")

    if scrape_rentals:
        logger.info("Scraping all JK rentals...")
        rental_results = scrape_all_jk_rentals(db_path, max_pages)

        rental_total = sum(rental_results.values())
        rental_successful = len([r for r in rental_results.values() if r > 0])
        logger.info(
            f"Rental scraping completed: {rental_successful}/{len(rental_results)} JKs successful, {rental_total} flats saved")

    if scrape_sales:
        logger.info("Scraping all JK sales...")
        sales_results = scrape_all_jk_sales(db_path, max_pages)

        sales_total = sum(sales_results.values())
        sales_successful = len([r for r in sales_results.values() if r > 0])
        logger.info(
            f"Sales scraping completed: {sales_successful}/{len(sales_results)} JKs successful, {sales_total} flats saved")

    logger.info("Immediate scraping completed!")


def fetch_all_jks(db_path: str = "flats.db") -> int:
    """
    Fetch all residential complexes (JKs) from Krisha.kz and save them to the database.
    First clears the existing data to ensure fresh, complete data.
    
    :param db_path: str, path to database file
    :return: int, number of JKs fetched and saved
    """
    logger.info("Starting fetch of all residential complexes from Krisha.kz...")

    try:
        # First, clear existing residential complexes data
        logger.info("🗑️ Clearing existing residential complexes data...")
        db = OrthancDB(db_path)
        db.connect()
        try:
            # Delete all existing residential complexes
            cursor = db.conn.execute("DELETE FROM residential_complexes")
            deleted_count = cursor.rowcount
            db.conn.commit()
            logger.info(f"✅ Cleared {deleted_count} existing residential complexes")
        except Exception as e:
            logger.error(f"❌ Error clearing existing data: {e}")
            db.conn.rollback()
            return 0
        finally:
            db.disconnect()

        # Now fetch fresh data from Krisha
        logger.info("🔄 Fetching fresh residential complexes from Krisha.kz...")
        saved_count = update_complex_database(db_path)

        if saved_count > 0:
            logger.info(f"✅ Successfully fetched and saved {saved_count} residential complexes")

            # Get some statistics about what was saved
            db = OrthancDB(db_path)
            db.connect()
            try:
                # Get total count
                cursor = db.conn.execute("SELECT COUNT(*) FROM residential_complexes")
                total_count = cursor.fetchone()[0]

                # Get some sample data
                cursor = db.conn.execute("""
                    SELECT name, complex_id, city, district 
                    FROM residential_complexes 
                    ORDER BY name 
                    LIMIT 10
                """)
                sample_complexes = cursor.fetchall()

                logger.info(f"📊 Database now contains {total_count} total residential complexes")
                logger.info("📋 Sample complexes:")
                for i, (name, complex_id, city, district) in enumerate(sample_complexes, 1):
                    location_info = f" ({city}, {district})" if city and district else ""
                    logger.info(f"   {i}. {name} (ID: {complex_id}){location_info}")

            finally:
                db.disconnect()

            return saved_count
        else:
            logger.warning("⚠️ No residential complexes were fetched")
            return 0

    except Exception as e:
        logger.error(f"❌ Error fetching residential complexes: {e}")
        return 0


def scrape_single_jk(jk_name: str, db_path: str = "flats.db", max_pages: int = 10,
                     scrape_rentals: bool = True, scrape_sales: bool = True) -> Dict[str, int]:
    """
    Scrape a specific JK if it doesn't already have data in the database.
    
    :param jk_name: str, name of the residential complex to scrape
    :param db_path: str, path to database file
    :param max_pages: int, maximum pages to scrape per JK
    :param scrape_rentals: bool, whether to scrape rentals
    :param scrape_sales: bool, whether to scrape sales
    :return: Dict[str, int], results with saved counts for rentals and sales
    """
    logger.info(f"Checking if {jk_name} already has data in database...")

    db = OrthancDB(db_path)
    db.connect()

    try:

        results = {
            'rentals_saved': 0,
            'sales_saved': 0,
        }

        if scrape_rentals:
            logger.info(f"📥 No rental data found for {jk_name}. Scraping rentals...")
            results['rentals_saved'] = scrape_and_save_jk_rentals(
                jk_name=jk_name,
                max_pages=max_pages,
                db_path=db_path
            )
            logger.info(f"✅ Saved {results['rentals_saved']} rental flats for {jk_name}")

        if scrape_sales:
            logger.info(f"📥 No sales data found for {jk_name}. Scraping sales...")
            results['sales_saved'] = scrape_and_save_jk_sales(
                jk_name=jk_name,
                max_pages=max_pages,
                db_path=db_path
            )
            logger.info(f"✅ Saved {results['sales_saved']} sales flats for {jk_name}")

        return results

    finally:
        db.disconnect()


def manage_blacklist(db_path: str = "flats.db", action: str = "list",
                     krisha_id: str = None, name: str = None, notes: str = None) -> None:
    """
    Manage blacklisted JKs.
    
    :param db_path: str, path to database file
    :param action: str, action to perform ('list', 'add', 'remove')
    :param krisha_id: str, Krisha ID of the JK
    :param name: str, name of the JK
    :param notes: str, notes for blacklisting
    """
    db = OrthancDB(db_path)

    if action == "list":
        blacklisted = db.get_blacklisted_jks()
        if blacklisted:
            logger.info(f"Blacklisted JKs ({len(blacklisted)}):")
            for jk in blacklisted:
                logger.info(f"  - {jk['name']} (ID: {jk['krisha_id']}) - {jk['notes'] or 'No notes'}")
        else:
            logger.info("No blacklisted JKs found")

    elif action == "add":
        if not krisha_id or not name:
            logger.error("Both krisha_id and name are required for adding to blacklist")
            return

        success = db.blacklist_jk(krisha_id, name, notes)
        if success:
            logger.info(f"Successfully blacklisted JK: {name}")
        else:
            logger.error(f"Failed to blacklist JK: {name}")

    elif action == "remove":
        if not krisha_id and not name:
            logger.error("Either krisha_id or name is required for removing from blacklist")
            return

        success = db.whitelist_jk(krisha_id, name)
        if success:
            logger.info(f"Successfully whitelisted JK: {name or krisha_id}")
        else:
            logger.error(f"Failed to whitelist JK: {name or krisha_id}")

    else:
        logger.error(f"Unknown action: {action}. Use 'list', 'add', or 'remove'")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Launch JK scraping operations")
    parser.add_argument("--mode",
                        choices=["immediate", "daily-rentals", "daily-sales", "blacklist", "fetch-jks", "scrape-jk"],
                        default="immediate", help="Scraping mode")
    parser.add_argument("--db-path", default="flats.db", help="Database file path")
    parser.add_argument("--max-pages", type=int, default=1, help="Maximum pages per JK")
    parser.add_argument("--rentals", action="store_true", help="Include rentals (immediate mode or scrape-jk mode)")
    parser.add_argument("--sales", action="store_true", help="Include sales (immediate mode or scrape-jk mode)")
    parser.add_argument("--run-time", default="12:00", help="Time to run daily scraping (HH:MM)")

    # Blacklist management arguments
    parser.add_argument("--blacklist-action", choices=["list", "add", "remove"],
                        default="list", help="Blacklist action")
    parser.add_argument("--krisha-id", help="Krisha ID for blacklist operations")
    parser.add_argument("--jk-name", help="JK name for blacklist operations or scrape-jk mode")
    parser.add_argument("--notes", help="Notes for blacklisting")

    args = parser.parse_args()

    if args.mode == "immediate":
        run_immediate_scraping(
            db_path=args.db_path,
            max_pages=args.max_pages,
            scrape_rentals=args.rentals or not (args.rentals or args.sales),
            scrape_sales=args.sales or not (args.rentals or args.sales)
        )
    elif args.mode == "daily-rentals":
        daily_rental_scraping_loop(
            db_path=args.db_path,
            max_pages=args.max_pages,
            run_time=args.run_time
        )
    elif args.mode == "daily-sales":
        daily_sales_scraping_loop(
            db_path=args.db_path,
            max_pages=args.max_pages,
            run_time=args.run_time
        )
    elif args.mode == "blacklist":
        manage_blacklist(
            db_path=args.db_path,
            action=args.blacklist_action,
            krisha_id=args.krisha_id,
            name=args.jk_name,
            notes=args.notes
        )
    elif args.mode == "fetch-jks":
        saved_count = fetch_all_jks(db_path=args.db_path)
        if saved_count > 0:
            logger.info(f"🎉 Successfully fetched {saved_count} residential complexes!")
        else:
            logger.error("❌ Failed to fetch residential complexes")
            sys.exit(1)
    elif args.mode == "scrape-jk":
        if not args.jk_name:
            logger.error("❌ --jk-name is required for scrape-jk mode")
            sys.exit(1)

        scrape_rentals = args.rentals or not (args.rentals or args.sales)
        scrape_sales = args.sales or not (args.rentals or args.sales)

        results = scrape_single_jk(
            jk_name=args.jk_name,
            db_path=args.db_path,
            max_pages=args.max_pages,
            scrape_rentals=scrape_rentals,
            scrape_sales=scrape_sales
        )
