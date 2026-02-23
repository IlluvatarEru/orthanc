"""
Launch script for scraping all JK rentals and sales.

This module provides functions to scrape all residential complexes (JKs)
from the database, both for rentals and sales, with daily scheduling options.

python -m scrapers.launch.launch_scraping_all_jks
"""

import sys
import time
from datetime import datetime, timedelta
from typing import List, Dict

from common.src.logging_config import setup_logging
from db.src.write_read_database import OrthancDB
from scrapers.src.krisha_rental_scraping import scrape_and_save_jk_rentals
from scrapers.src.krisha_sales_scraping import scrape_and_save_jk_sales
from scrapers.src.residential_complex_scraper import (
    update_complex_database,
    update_jks_with_unknown_cities,
    scrape_districts_for_city,
    CITY_DISTRICTS,
)
from scrapers.src.utils import get_and_reset_error_counts, get_scraping_config, throttle

logger = setup_logging(__name__, log_file="jk_scraping.log")


def _prioritize_jks(jks: List[Dict]) -> List[Dict]:
    """
    Sort JKs so that priority JKs (from config) come first.
    Priority is determined by case-insensitive partial match on JK name.

    :param jks: List[Dict], list of JK information
    :return: List[Dict], sorted with priority JKs first
    """
    config = get_scraping_config()
    priority_patterns = config.get("priority_jks", [])
    if not priority_patterns:
        return jks

    priority = []
    rest = []
    for jk in jks:
        name = jk.get("residential_complex", "").lower()
        if any(p.lower() in name for p in priority_patterns):
            priority.append(jk)
        else:
            rest.append(jk)

    if priority:
        names = [jk["residential_complex"] for jk in priority]
        logger.info(f"Priority JKs ({len(priority)}): {names}")

    return priority + rest


def _log_scraping_summary(db: OrthancDB, jks: List[Dict], city: str = None) -> None:
    """
    Log a summary of how many JKs will be scraped and how many were excluded.

    :param db: OrthancDB, database instance
    :param jks: List[Dict], JKs that will be scraped (after all filtering)
    :param city: str, city filter applied (None = all cities)
    """
    blacklisted_jks = db.get_blacklisted_jks()
    blacklisted_districts = db.get_blacklisted_districts()
    excluded_count = db.get_residential_complexes_count() - len(jks)

    city_label = city or "all cities"
    district_names = [f"{d['district']} ({d['city']})" for d in blacklisted_districts]

    logger.info("=" * 60)
    logger.info(f"SCRAPING SUMMARY ({city_label})")
    logger.info(f"  JKs to scrape:          {len(jks)}")
    logger.info(f"  Excluded:               {excluded_count}")
    if blacklisted_jks:
        logger.info(
            f"    Blacklisted JKs:      {[jk['name'] for jk in blacklisted_jks]}"
        )
    if blacklisted_districts:
        logger.info(f"    Blacklisted districts: {district_names}")
    logger.info("=" * 60)


def get_all_jks_from_db(db_path: str = "flats.db") -> List[Dict]:
    """
    Get all residential complexes (JKs) from the residential_complexes table, excluding blacklisted ones.

    :param db_path: str, path to database file
    :return: List[Dict], list of JK information
    """
    logger.info(
        "Fetching all JKs from residential_complexes table (excluding blacklisted)"
    )

    db = OrthancDB(db_path)
    jks = db.get_all_jks_excluding_blacklisted_no_city_filter()

    _log_scraping_summary(db, jks)

    return jks


def scrape_all_jk_rentals(
    db_path: str = "flats.db", max_pages: int = 10
) -> Dict[str, int]:
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

    throttle.reset()
    results = {}
    total_saved = 0

    for i, jk_info in enumerate(jks, 1):
        if throttle.is_circuit_broken:
            remaining = len(jks) - i + 1
            logger.warning(
                f"Circuit breaker: stopping early. "
                f"Skipping {remaining} remaining JKs."
            )
            break

        jk_name = jk_info["residential_complex"]
        jk_city = jk_info.get("city", "almaty") or "almaty"
        progress_pct = (i / len(jks)) * 100
        logger.info(
            f"[{i}/{len(jks)}] ({progress_pct:.1f}%) Scraping rentals for: {jk_name}"
        )

        try:
            saved_count = scrape_and_save_jk_rentals(
                jk_name=jk_name, max_pages=max_pages, db_path=db_path, city=jk_city
            )

            results[jk_name] = saved_count
            total_saved += saved_count
            throttle.record_jk_result(saved_count)

            logger.info(f"  {jk_name}: {saved_count} rental flats saved")

            # Add delay between JKs to be respectful
            time.sleep(2)

        except Exception as e:
            logger.error(f"  Error scraping {jk_name}: {e}")
            results[jk_name] = 0
            throttle.record_jk_result(0)

    logger.info(f"Completed JK rental scraping. Total saved: {total_saved}")
    return results


def scrape_all_jk_sales(
    db_path: str = "flats.db",
    max_pages: int = 10,
    city: str = None,
    exclude_city: str = None,
) -> Dict:
    """
    Scrape all JK sales from the database.

    :param db_path: str, path to database file
    :param max_pages: int, maximum pages to scrape per JK
    :param city: str, city name in Cyrillic to include only (e.g. "Алматы"), None for all
    :param exclude_city: str, city name in Cyrillic to exclude (e.g. "Алматы"), None for none
    :return: Dict with keys: results, started_at, finished_at, duration_seconds,
             city, jks_total, jks_successful, jks_failed, flats_scraped,
             rate_limited, http_errors, request_errors
    """
    # Reset error counters and throttle state before this run
    get_and_reset_error_counts()
    throttle.reset()
    start_time = time.time()
    started_at = datetime.now().isoformat()

    if city:
        logger.info(f"Starting scraping of JK sales for city: {city}")
    elif exclude_city:
        logger.info(f"Starting scraping of JK sales excluding city: {exclude_city}")
    else:
        logger.info("Starting scraping of all JK sales")

    db = OrthancDB(db_path)
    jks = db.get_all_jks_excluding_blacklisted_no_city_filter()
    if not jks:
        logger.warning("No JKs found in database")
        finished_at = datetime.now().isoformat()
        return {
            "results": {},
            "started_at": started_at,
            "finished_at": finished_at,
            "duration_seconds": time.time() - start_time,
            "city": city,
            "jks_total": 0,
            "jks_successful": 0,
            "jks_failed": 0,
            "flats_scraped": 0,
            "rate_limited": 0,
            "http_errors": 0,
            "request_errors": 0,
        }

    if city:
        jks = [jk for jk in jks if jk.get("city") == city]
    elif exclude_city:
        jks = [jk for jk in jks if jk.get("city") != exclude_city]

    _log_scraping_summary(db, jks, city=city or exclude_city)

    jks = _prioritize_jks(jks)

    results = {}
    total_saved = 0
    jks_failed = 0

    circuit_broken = False
    for i, jk_info in enumerate(jks, 1):
        # Check circuit breaker before each JK
        if throttle.is_circuit_broken:
            remaining = len(jks) - i + 1
            logger.warning(
                f"Circuit breaker: stopping early. "
                f"Skipping {remaining} remaining JKs."
            )
            circuit_broken = True
            break

        jk_name = jk_info["residential_complex"]
        jk_city = jk_info.get("city", "almaty") or "almaty"
        progress_pct = (i / len(jks)) * 100
        logger.info(
            f"[{i}/{len(jks)}] ({progress_pct:.1f}%) Scraping sales for: {jk_name}"
        )

        try:
            saved_count = scrape_and_save_jk_sales(
                jk_name=jk_name, max_pages=max_pages, db_path=db_path, city=jk_city
            )

            results[jk_name] = saved_count
            total_saved += saved_count
            throttle.record_jk_result(saved_count)

            logger.info(f"  {jk_name}: {saved_count} sales flats saved")

            # Add delay between JKs to be respectful
            time.sleep(2)

        except Exception as e:
            logger.error(f"  Error scraping {jk_name}: {e}")
            results[jk_name] = 0
            jks_failed += 1
            throttle.record_jk_result(0)

    finished_at = datetime.now().isoformat()
    duration = time.time() - start_time
    jks_successful = len([r for r in results.values() if r > 0])
    error_counts = get_and_reset_error_counts()

    status_msg = "Circuit breaker stopped run early" if circuit_broken else "Completed"
    logger.info(f"{status_msg}. Total saved: {total_saved}")
    logger.info(
        f"Stats: {len(results)} JKs processed, {jks_successful} successful, "
        f"{jks_failed} failed, {total_saved} flats, {duration:.1f}s"
    )
    if error_counts:
        logger.warning(f"Error breakdown: {error_counts}")

    return {
        "results": results,
        "started_at": started_at,
        "finished_at": finished_at,
        "duration_seconds": round(duration, 1),
        "city": city,
        "jks_total": len(results),
        "jks_successful": jks_successful,
        "jks_failed": jks_failed,
        "flats_scraped": total_saved,
        "error_breakdown": error_counts,
    }


def _calculate_seconds_until(target_hour: int, target_minute: int) -> float:
    """
    Calculate seconds from now until the next occurrence of target_hour:target_minute.

    :param target_hour: int, target hour (0-23)
    :param target_minute: int, target minute (0-59)
    :return: float, seconds until target time
    """
    now = datetime.now()
    next_run = now.replace(
        hour=target_hour, minute=target_minute, second=0, microsecond=0
    )
    if next_run <= now:
        next_run += timedelta(days=1)
    return (next_run - now).total_seconds()


def daily_rental_scraping_loop(
    db_path: str = "flats.db", max_pages: int = 10, run_time: str = "12:00"
) -> None:
    """
    Run daily rental scraping in a continuous loop.
    Starts immediately when launched, then runs at the specified time each day.

    :param db_path: str, path to database file
    :param max_pages: int, maximum pages to scrape per JK
    :param run_time: str, time to run scraping (format: "HH:MM")
    """
    logger.info(
        f"Starting daily rental scraping loop (immediate run, then daily at {run_time})"
    )

    # Run immediately on startup
    logger.info("Running initial rental scraping immediately...")
    try:
        results = scrape_all_jk_rentals(db_path, max_pages)

        # Log summary
        total_saved = sum(results.values())
        successful_jks = len([r for r in results.values() if r > 0])

        logger.info("Initial rental scraping completed:")
        logger.info(f"  - JKs processed: {len(results)}")
        logger.info(f"  - Successful JKs: {successful_jks}")
        logger.info(f"  - Total flats saved: {total_saved}")
    except Exception as e:
        logger.error(f"Error in initial rental scraping: {e}")

    # Parse target time
    target_hour, target_minute = map(int, run_time.split(":"))

    while True:
        try:
            # Sleep until exact target time
            wait_seconds = _calculate_seconds_until(target_hour, target_minute)
            next_run = datetime.now() + timedelta(seconds=wait_seconds)
            logger.info(
                f"Next rental scraping at {next_run.strftime('%Y-%m-%d %H:%M:%S')} "
                f"(sleeping {wait_seconds / 3600:.1f} hours)"
            )
            time.sleep(wait_seconds)

            logger.info(
                f"Starting daily rental scraping at {datetime.now().strftime('%H:%M')}"
            )
            results = scrape_all_jk_rentals(db_path, max_pages)

            # Log summary
            total_saved = sum(results.values())
            successful_jks = len([r for r in results.values() if r > 0])

            logger.info("Daily rental scraping completed:")
            logger.info(f"  - JKs processed: {len(results)}")
            logger.info(f"  - Successful JKs: {successful_jks}")
            logger.info(f"  - Total flats saved: {total_saved}")

        except KeyboardInterrupt:
            logger.info("Daily rental scraping loop stopped by user")
            break
        except Exception as e:
            logger.error(f"Error in daily rental scraping loop: {e}")
            time.sleep(300)  # Wait 5 minutes before retrying


def daily_sales_scraping_loop(
    db_path: str = "flats.db", max_pages: int = 10, run_time: str = "13:00"
) -> None:
    """
    Run daily sales scraping in a continuous loop.
    Starts immediately when launched, then runs at the specified time each day.

    :param db_path: str, path to database file
    :param max_pages: int, maximum pages to scrape per JK
    :param run_time: str, time to run scraping (format: "HH:MM")
    """
    logger.info(
        f"Starting daily sales scraping loop (immediate run, then daily at {run_time})"
    )

    # Run immediately on startup
    logger.info("Running initial sales scraping immediately...")
    try:
        stats = scrape_all_jk_sales(db_path, max_pages)

        logger.info("Initial sales scraping completed:")
        logger.info(f"  - JKs processed: {stats['jks_total']}")
        logger.info(f"  - Successful JKs: {stats['jks_successful']}")
        logger.info(f"  - Total flats saved: {stats['flats_scraped']}")
    except Exception as e:
        logger.error(f"Error in initial sales scraping: {e}")

    # Parse target time
    target_hour, target_minute = map(int, run_time.split(":"))

    while True:
        try:
            # Sleep until exact target time
            wait_seconds = _calculate_seconds_until(target_hour, target_minute)
            next_run = datetime.now() + timedelta(seconds=wait_seconds)
            logger.info(
                f"Next sales scraping at {next_run.strftime('%Y-%m-%d %H:%M:%S')} "
                f"(sleeping {wait_seconds / 3600:.1f} hours)"
            )
            time.sleep(wait_seconds)

            logger.info(
                f"Starting daily sales scraping at {datetime.now().strftime('%H:%M')}"
            )
            stats = scrape_all_jk_sales(db_path, max_pages)

            logger.info("Daily sales scraping completed:")
            logger.info(f"  - JKs processed: {stats['jks_total']}")
            logger.info(f"  - Successful JKs: {stats['jks_successful']}")
            logger.info(f"  - Total flats saved: {stats['flats_scraped']}")

        except KeyboardInterrupt:
            logger.info("Daily sales scraping loop stopped by user")
            break
        except Exception as e:
            logger.error(f"Error in daily sales scraping loop: {e}")
            time.sleep(300)  # Wait 5 minutes before retrying


def run_immediate_scraping(
    db_path: str = "flats.db",
    max_pages: int = 10,
    scrape_rentals: bool = True,
    scrape_sales: bool = True,
    city: str = None,
    exclude_city: str = None,
) -> None:
    """
    Run immediate scraping of all JKs (rentals and/or sales).

    :param db_path: str, path to database file
    :param max_pages: int, maximum pages to scrape per JK
    :param scrape_rentals: bool, whether to scrape rentals
    :param scrape_sales: bool, whether to scrape sales
    :param city: str, city name in Cyrillic to include only (e.g. "Алматы"), None for all
    :param exclude_city: str, city name in Cyrillic to exclude, None for none
    """
    city_label = city or (
        "all except " + exclude_city if exclude_city else "all cities"
    )
    logger.info(f"Starting immediate scraping of JKs ({city_label})")

    if scrape_rentals:
        logger.info("Scraping all JK rentals...")
        rental_results = scrape_all_jk_rentals(db_path, max_pages)

        rental_total = sum(rental_results.values())
        rental_successful = len([r for r in rental_results.values() if r > 0])
        logger.info(
            f"Rental scraping completed: {rental_successful}/{len(rental_results)} JKs successful, {rental_total} flats saved"
        )

    if scrape_sales:
        logger.info(f"Scraping JK sales ({city_label})...")
        sales_stats = scrape_all_jk_sales(
            db_path, max_pages, city=city, exclude_city=exclude_city
        )

        logger.info(
            f"Sales scraping completed: {sales_stats['jks_successful']}/{sales_stats['jks_total']} JKs successful, "
            f"{sales_stats['flats_scraped']} flats saved, {sales_stats['duration_seconds']}s"
        )

        # Save run stats to database
        db = OrthancDB(db_path)
        db.insert_pipeline_run(sales_stats)

    logger.info("Immediate scraping completed!")


def fetch_all_jks(db_path: str = "flats.db") -> int:
    """
    Fetch all residential complexes (JKs) from Krisha.kz and save them to the database.
    Only adds new JKs or updates existing ones if their city is NULL or "Unknown".

    :param db_path: str, path to database file
    :return: int, number of JKs fetched and saved/updated
    """
    logger.info("Starting fetch of all residential complexes from Krisha.kz...")

    try:
        # Fetch fresh data from Krisha and update database
        # This will only add new JKs or update existing ones with NULL/Unknown cities
        logger.info("🔄 Fetching residential complexes from Krisha.kz...")
        saved_count = update_complex_database(db_path)

        if saved_count > 0:
            logger.info(
                f"✅ Successfully fetched and saved {saved_count} residential complexes"
            )

            # Get some statistics about what was saved
            db = OrthancDB(db_path)
            try:
                # Get total count
                total_count = db.get_residential_complexes_count()

                # Get some sample data
                sample_complexes = db.get_sample_residential_complexes(limit=10)

                logger.info(
                    f"📊 Database now contains {total_count} total residential complexes"
                )
                logger.info("📋 Sample complexes:")
                for i, complex_data in enumerate(sample_complexes, 1):
                    name = complex_data["name"]
                    complex_id = complex_data["complex_id"]
                    city = complex_data.get("city")
                    district = complex_data.get("district")
                    location_info = (
                        f" ({city}, {district})" if city and district else ""
                    )
                    logger.info(f"   {i}. {name} (ID: {complex_id}){location_info}")

            except Exception as e:
                logger.error(f"Error getting statistics: {e}")

            return saved_count
        else:
            logger.warning("⚠️ No residential complexes were fetched")
            return 0

    except Exception as e:
        logger.error(f"❌ Error fetching residential complexes: {e}")
        return 0


def scrape_single_jk(
    jk_name: str,
    db_path: str = "flats.db",
    max_pages: int = 10,
    scrape_rentals: bool = False,
    scrape_sales: bool = False,
) -> None:
    """
    Scrape a single JK (rentals and/or sales).
    Uses fuzzy matching to find all JKs that match the search term.

    :param jk_name: str, name of the residential complex to scrape (supports partial match)
    :param db_path: str, path to database file
    :param max_pages: int, maximum pages to scrape per JK
    :param scrape_rentals: bool, whether to scrape rentals
    :param scrape_sales: bool, whether to scrape sales
    """
    if not scrape_rentals and not scrape_sales:
        logger.error("At least one of --rentals or --sales must be specified")
        return

    # Find all matching JK names using fuzzy matching
    db = OrthancDB(db_path)
    matching_jks = db.find_matching_jk_names(jk_name)

    if not matching_jks:
        logger.warning(f"No JKs found matching '{jk_name}'. Trying exact name...")
        matching_jks = [jk_name]
    else:
        logger.info(
            f"Found {len(matching_jks)} JKs matching '{jk_name}': {matching_jks}"
        )

    total_rentals_saved = 0
    total_sales_saved = 0

    for jk in matching_jks:
        logger.info(f"Starting scraping for JK: {jk}")

        if scrape_rentals:
            logger.info(f"Scraping rentals for: {jk}")
            saved_count = scrape_and_save_jk_rentals(
                jk_name=jk, max_pages=max_pages, db_path=db_path
            )
            total_rentals_saved += saved_count
            logger.info(
                f"✅ Rental scraping completed for {jk}: {saved_count} flats saved"
            )

        if scrape_sales:
            logger.info(f"Scraping sales for: {jk}")
            saved_count = scrape_and_save_jk_sales(
                jk_name=jk, max_pages=max_pages, db_path=db_path
            )
            total_sales_saved += saved_count
            logger.info(
                f"✅ Sales scraping completed for {jk}: {saved_count} flats saved"
            )

    logger.info(f"Scraping completed for '{jk_name}' ({len(matching_jks)} JKs)")
    if scrape_rentals:
        logger.info(f"Total rentals saved: {total_rentals_saved}")
    if scrape_sales:
        logger.info(f"Total sales saved: {total_sales_saved}")


def manage_blacklist(
    db_path: str = "flats.db",
    action: str = "list",
    krisha_id: str = None,
    name: str = None,
    notes: str = None,
) -> None:
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
                logger.info(
                    f"  - {jk['name']} (ID: {jk['krisha_id']}) - {jk['notes'] or 'No notes'}"
                )
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
            logger.error(
                "Either krisha_id or name is required for removing from blacklist"
            )
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
    parser.add_argument(
        "--mode",
        choices=[
            "immediate",
            "daily-rentals",
            "daily-sales",
            "blacklist",
            "fetch-jks",
            "scrape-jk",
            "update-jks-cities",
            "update-districts",
        ],
        default="immediate",
        help="Scraping mode",
    )
    parser.add_argument("--db-path", default="flats.db", help="Database file path")
    parser.add_argument(
        "--max-pages", type=int, default=10, help="Maximum pages per JK"
    )
    parser.add_argument(
        "--rentals", action="store_true", help="Include rentals (immediate mode)"
    )
    parser.add_argument(
        "--sales", action="store_true", help="Include sales (immediate mode)"
    )
    parser.add_argument(
        "--run-time", default="12:00", help="Time to run daily scraping (HH:MM)"
    )

    # Blacklist management arguments
    parser.add_argument(
        "--blacklist-action",
        choices=["list", "add", "remove"],
        default="list",
        help="Blacklist action",
    )
    parser.add_argument(
        "--city",
        help="Only scrape this city (e.g. almaty, astana). Default: all cities",
    )
    parser.add_argument(
        "--exclude-city",
        help="Scrape all cities except this one (e.g. almaty). Cannot combine with --city",
    )
    parser.add_argument("--krisha-id", help="Krisha ID for blacklist operations")
    parser.add_argument("--jk-name", help="JK name for blacklist operations")
    parser.add_argument("--notes", help="Notes for blacklisting")

    args = parser.parse_args()

    # Map city shorthand to Cyrillic
    city_map = {"almaty": "Алматы", "astana": "Астане", "all": None}
    city = city_map.get(args.city, args.city) if args.city else None
    exclude_city = (
        city_map.get(args.exclude_city, args.exclude_city)
        if args.exclude_city
        else None
    )

    if args.mode == "immediate":
        run_immediate_scraping(
            db_path=args.db_path,
            max_pages=args.max_pages,
            scrape_rentals=args.rentals or not (args.rentals or args.sales),
            scrape_sales=args.sales or not (args.rentals or args.sales),
            city=city,
            exclude_city=exclude_city,
        )
    elif args.mode == "daily-rentals":
        daily_rental_scraping_loop(
            db_path=args.db_path, max_pages=args.max_pages, run_time=args.run_time
        )
    elif args.mode == "daily-sales":
        daily_sales_scraping_loop(
            db_path=args.db_path, max_pages=args.max_pages, run_time=args.run_time
        )
    elif args.mode == "blacklist":
        manage_blacklist(
            db_path=args.db_path,
            action=args.blacklist_action,
            krisha_id=args.krisha_id,
            name=args.jk_name,
            notes=args.notes,
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
            logger.error("--jk-name is required when using --mode scrape-jk")
            sys.exit(1)
        scrape_single_jk(
            jk_name=args.jk_name,
            db_path=args.db_path,
            max_pages=args.max_pages,
            scrape_rentals=args.rentals,
            scrape_sales=args.sales,
        )
    elif args.mode == "update-jks-cities":
        logger.info("Updating JKs with unknown cities...")
        updated_count = update_jks_with_unknown_cities(db_path=args.db_path)
        if updated_count > 0:
            logger.info(
                f"✅ Successfully updated {updated_count} JKs with city information!"
            )
        else:
            logger.info("ℹ️ No JKs needed updating or no cities could be determined")
    elif args.mode == "update-districts":
        logger.info("Updating district info for JKs...")
        total = 0
        for city in CITY_DISTRICTS:
            total += scrape_districts_for_city(city, db_path=args.db_path)
        logger.info(f"Updated {total} JKs with district information")
