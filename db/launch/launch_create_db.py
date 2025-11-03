#!/usr/bin/env python3
"""
Database initialization script for Orthanc project.

This script creates and initializes the complete database schema
with all tables, indexes, and initial data.

Usage:
    python -m db.launch.launch_create_db
    python -m db.launch.launch_create_db --db-path custom_flats.db
    python -m db.launch.launch_create_db --force  # Drop existing tables first
"""

import sys
import os
import argparse
import logging

# Add the project root to the Python path
sys.path.insert(
    0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
)

from db.src.table_creation import DatabaseSchema
from db.src.write_read_database import OrthancDB

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def create_database(db_path: str = "flats.db", force: bool = False) -> bool:
    """
    Create and initialize the complete database schema.

    :param db_path: str, path to database file
    :param force: bool, whether to drop existing tables first
    :return: bool, True if successful
    """
    logger.info(f"Creating database schema at: {db_path}")

    try:
        # Initialize database schema
        schema = DatabaseSchema(db_path)

        if force:
            logger.info("Dropping existing tables...")
            schema.drop_tables()

        # Create all tables and indexes
        logger.info("Creating database tables...")
        schema.create_tables()

        logger.info("Creating database indexes...")
        schema.create_indexes()

        # Verify database creation
        logger.info("Verifying database creation...")
        info = schema.get_table_info()

        logger.info("✅ Database created successfully!")
        logger.info("📊 Database Statistics:")
        for table_name, table_data in info.items():
            logger.info(f"  - {table_name}: {table_data['row_count']} rows")

        return True

    except Exception as e:
        logger.error(f"❌ Error creating database: {e}")
        return False


def add_sample_data(db_path: str = "flats.db") -> bool:
    """
    Add sample data to the database for development/testing.

    :param db_path: str, path to database file
    :return: bool, True if successful
    """
    logger.info("Adding sample data...")

    try:
        db = OrthancDB(db_path)

        # Add sample residential complexes
        sample_complexes = [
            ("jk_001", "Meridian Apartments", "Almaty", "Medeu District"),
            ("jk_002", "Nurly Tau", "Almaty", "Alatau District"),
            ("jk_003", "Samal Towers", "Almaty", "Samal District"),
            ("jk_004", "Koktem Residential", "Almaty", "Koktem District"),
            ("jk_005", "Aksai Complex", "Almaty", "Aksai District"),
        ]

        for complex_id, name, city, district in sample_complexes:
            db.insert_residential_complex(complex_id, name, city, district)
            logger.info(f"Added sample complex: {name}")

        # Add sample exchange rates
        db.insert_exchange_rate("USD", 450.0)
        db.insert_exchange_rate("EUR", 480.0)
        logger.info("Added sample exchange rates")

        logger.info("✅ Sample data added successfully!")
        return True

    except Exception as e:
        logger.error(f"❌ Error adding sample data: {e}")
        return False


def verify_database(db_path: str = "flats.db") -> bool:
    """
    Verify that the database is properly set up.

    :param db_path: str, path to database file
    :return: bool, True if database is valid
    """
    logger.info("Verifying database setup...")

    try:
        db = OrthancDB(db_path)

        # Check if all required tables exist
        required_tables = [
            "residential_complexes",
            "rental_flats",
            "sales_flats",
            "favorites",
            "blacklisted_jks",
            "mid_prices",
            "jk_performance_snapshots",
        ]

        db.connect()
        cursor = db.conn.execute("""
            SELECT name FROM sqlite_master 
            WHERE type='table' AND name NOT LIKE 'sqlite_%'
        """)
        existing_tables = [row[0] for row in cursor.fetchall()]

        missing_tables = set(required_tables) - set(existing_tables)
        if missing_tables:
            logger.error(f"❌ Missing tables: {missing_tables}")
            return False

        logger.info("✅ All required tables exist")

        # Test basic operations
        complexes = db.get_all_residential_complexes()
        blacklisted = db.get_blacklisted_jks()
        currencies = db.get_all_currencies()

        logger.info("✅ Database verification successful:")
        logger.info(f"  - Residential complexes: {len(complexes)}")
        logger.info(f"  - Blacklisted JKs: {len(blacklisted)}")
        logger.info(f"  - Currencies: {currencies}")

        return True

    except Exception as e:
        logger.error(f"❌ Database verification failed: {e}")
        return False
    finally:
        db.disconnect()


def main():
    """
    Main function for database initialization.
    """
    parser = argparse.ArgumentParser(
        description="Initialize Orthanc database with complete schema",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Create database with default settings
  python -m db.launch.launch_create_db
  
  # Create database at custom path
  python -m db.launch.launch_create_db --db-path custom_flats.db
  
  # Force recreate (drop existing tables)
  python -m db.launch.launch_create_db --force
  
  # Create with sample data
  python -m db.launch.launch_create_db --sample-data
  
  # Full recreation with sample data
  python -m db.launch.launch_create_db --force --sample-data
        """,
    )

    parser.add_argument(
        "--db-path",
        default="flats.db",
        help="Path to database file (default: flats.db)",
    )

    parser.add_argument(
        "--force",
        action="store_true",
        help="Drop existing tables before creating new ones",
    )

    parser.add_argument(
        "--sample-data",
        action="store_true",
        help="Add sample data for development/testing",
    )

    parser.add_argument(
        "--verify-only",
        action="store_true",
        help="Only verify existing database (don't create)",
    )

    args = parser.parse_args()

    # Check if database already exists
    if os.path.exists(args.db_path) and not args.force and not args.verify_only:
        logger.warning(f"Database {args.db_path} already exists!")
        logger.info(
            "Use --force to recreate or --verify-only to check existing database"
        )
        return

    if args.verify_only:
        # Only verify existing database
        if verify_database(args.db_path):
            logger.info("✅ Database verification completed successfully!")
        else:
            logger.error("❌ Database verification failed!")
            sys.exit(1)
        return

    # Create database
    if not create_database(args.db_path, args.force):
        logger.error("❌ Database creation failed!")
        sys.exit(1)

    # Add sample data if requested
    if args.sample_data:
        if not add_sample_data(args.db_path):
            logger.error("❌ Sample data addition failed!")
            sys.exit(1)

    # Verify database
    if not verify_database(args.db_path):
        logger.error("❌ Database verification failed!")
        sys.exit(1)

    logger.info("🎉 Database initialization completed successfully!")
    logger.info(f"Database location: {os.path.abspath(args.db_path)}")
    logger.info(
        "You can now run scraping operations and use the blacklist management tools."
    )


if __name__ == "__main__":
    main()
