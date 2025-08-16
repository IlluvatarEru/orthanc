"""
Command-line interface for Krisha.kz scraper and database management.
"""
import csv
from argparse import ArgumentParser

from db.src.database import FlatDatabase
from scrapers.src.scraper_with_db import scrape_and_save, scrape_multiple_flats, get_database_summary, \
    search_flats_in_db
import logging

def scrape_single_flat(url: str, db_path: str = "flats.db"):
    """
    Scrape a single flat and save to database.
    
    :param url: str, URL of the flat to scrape
    :param db_path: str, database file path
    """
    logging.info(f"Scraping flat: {url}")
    flat_info = scrape_and_save(url, db_path)

    if flat_info:
        logging.info(f"Successfully scraped flat {flat_info.flat_id}")
        logging.info(f"   Price: {flat_info.price:,} tenge")
        logging.info(f"   Area: {flat_info.area} m²")
        logging.info(f"   Residential Complex: {flat_info.residential_complex or 'N/A'}")
    else:
        logging.info("Failed to scrape flat")


def scrape_from_file(file_path: str, db_path: str = "flats.db", delay: float = 2.0):
    """
    Scrape flats from a file containing URLs.
    
    :param file_path: str, path to file containing URLs (one per line)
    :param db_path: str, database file path
    :param delay: float, delay between requests
    """
    try:
        with open(file_path, 'r') as f:
            urls = [line.strip() for line in f if line.strip() and not line.strip().startswith('#')]

        logging.info(f"Found {len(urls)} URLs in {file_path}")
        scrape_multiple_flats(urls, db_path, delay)

    except FileNotFoundError:
        logging.info(f"File not found: {file_path}")
    except Exception as e:
        logging.info(f"Error reading file: {e}")


def show_stats(db_path: str = "flats.db"):
    """
    Show database statistics.
    
    :param db_path: str, database file path
    """
    get_database_summary(db_path)


def search_database(min_price: int = None, max_price: int = None,
                    min_area: float = None, max_area: float = None,
                    residential_complex: str = None, limit: int = None,
                    db_path: str = "flats.db"):
    """
    Search database with filters.
    
    :param min_price: int, minimum price
    :param max_price: int, maximum price
    :param min_area: float, minimum area
    :param max_area: float, maximum area
    :param residential_complex: str, residential complex name
    :param limit: int, maximum results
    :param db_path: str, database file path
    """
    search_flats_in_db(
        min_price=min_price,
        max_price=max_price,
        min_area=min_area,
        max_area=max_area,
        residential_complex=residential_complex,
        limit=limit,
        db_path=db_path
    )


def export_to_csv(output_file: str, db_path: str = "flats.db"):
    """
    Export database to CSV file.
    
    :param output_file: str, output CSV file path
    :param db_path: str, database file path
    """

    db = FlatDatabase(db_path)
    flats = db.get_all_flats()

    if not flats:
        logging.info("No data to export")
        return

    try:
        with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = ['flat_id', 'price', 'area', 'residential_complex', 'floor',
                          'total_floors', 'construction_year', 'parking', 'description',
                          'url', 'scraped_at']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

            writer.writeheader()
            for flat in flats:
                # Only include the fields we want in CSV
                csv_row = {field: flat.get(field, '') for field in fieldnames}
                writer.writerow(csv_row)

        logging.info(f"Exported {len(flats)} flats to {output_file}")

    except Exception as e:
        logging.info(f"Error exporting to CSV: {e}")


def main():
    """
    Main CLI function.
    """
    parser = ArgumentParser(description='Krisha.kz Scraper CLI Tool')
    parser.add_argument('--db', default='flats.db', help='Database file path')

    subparsers = parser.add_subparsers(dest='command', help='Available commands')

    # Scrape single flat
    scrape_parser = subparsers.add_parser('scrape', help='Scrape a single flat')
    scrape_parser.add_argument('url', help='URL of the flat to scrape')

    # Scrape from file
    file_parser = subparsers.add_parser('scrape-file', help='Scrape flats from file')
    file_parser.add_argument('file', help='File containing URLs (one per line)')
    file_parser.add_argument('--delay', type=float, default=2.0, help='Delay between requests')

    # Show statistics
    stats_parser = subparsers.add_parser('stats', help='Show database statistics')

    # Search database
    search_parser = subparsers.add_parser('search', help='Search database')
    search_parser.add_argument('--min-price', type=int, help='Minimum price')
    search_parser.add_argument('--max-price', type=int, help='Maximum price')
    search_parser.add_argument('--min-area', type=float, help='Minimum area')
    search_parser.add_argument('--max-area', type=float, help='Maximum area')
    search_parser.add_argument('--complex', help='Residential complex name')
    search_parser.add_argument('--limit', type=int, help='Maximum results')

    # Export to CSV
    export_parser = subparsers.add_parser('export', help='Export to CSV')
    export_parser.add_argument('output', help='Output CSV file path')

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return

    try:
        if args.command == 'scrape':
            scrape_single_flat(args.url, args.db)

        elif args.command == 'scrape-file':
            scrape_from_file(args.file, args.db, args.delay)

        elif args.command == 'stats':
            show_stats(args.db)

        elif args.command == 'search':
            search_database(
                min_price=args.min_price,
                max_price=args.max_price,
                min_area=args.min_area,
                max_area=args.max_area,
                residential_complex=args.complex,
                limit=args.limit,
                db_path=args.db
            )

        elif args.command == 'export':
            export_to_csv(args.output, args.db)

    except KeyboardInterrupt:
        logging.info("\nOperation cancelled by user")
    except Exception as e:
        logging.info(f"Error: {e}")


if __name__ == "__main__":
    main()
