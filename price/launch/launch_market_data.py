"""
Real-time market data launcher for fetching MIG exchange rates.

This module provides functionality to continuously fetch exchange rates
from mig.kz once daily at midday UTC and store them in the database.

python -m price.launch.launch_market_data
"""

import logging
import time
from datetime import datetime, timezone, timedelta
from typing import Dict

from common.src.dates import get_next_midday_utc
from db.src.write_read_database import OrthancDB
from price.src.currency import CurrencyManager


def launch_realtime_market_data(db_path: str = "flats.db", log_level: int = logging.INFO) -> None:
    """
    Launch real-time market data fetching service.
    
    Fetches MIG exchange rates once daily at midday UTC and stores them in the database.
    Runs continuously, sleeping until the next midday UTC.
    
    :param db_path: str, path to SQLite database file
    :param log_level: int, logging level (default: INFO)
    """
    # Configure logging
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler('market_data.log')
        ]
    )

    logger = logging.getLogger(__name__)
    logger.info("Starting real-time market data service")
    logger.info(f"Database path: {db_path}")

    # Initialize components
    currency_manager = CurrencyManager()
    db = OrthancDB(db_path)

    logger.info("Market data service initialized successfully")

    while True:
        try:
            # Calculate next midday UTC
            now = datetime.now(timezone.utc)
            next_midday = get_next_midday_utc(now)

            # Calculate sleep duration
            sleep_duration = (next_midday - now).total_seconds()

            logger.info(f"Current time: {now.strftime('%Y-%m-%d %H:%M:%S UTC')}")
            logger.info(f"Next fetch scheduled for: {next_midday.strftime('%Y-%m-%d %H:%M:%S UTC')}")
            logger.info(f"Sleeping for {sleep_duration / 3600:.1f} hours")

            # Sleep until next midday
            time.sleep(sleep_duration)

            # Fetch and store market data
            logger.info("Fetching market data at midday UTC")
            success = fetch_and_store_market_data(currency_manager, db, logger)

            if success:
                logger.info("Market data fetch completed successfully")
            else:
                logger.error("Market data fetch failed")

        except KeyboardInterrupt:
            logger.info("Received interrupt signal, shutting down gracefully")
            break
        except Exception as e:
            logger.error(f"Unexpected error in market data service: {e}")
            # Sleep for 1 hour before retrying on error
            logger.info("Sleeping for 1 hour before retry")
            time.sleep(3600)


def fetch_and_store_market_data(currency_manager: CurrencyManager, db: OrthancDB, logger: logging.Logger) -> bool:
    """
    Fetch exchange rates from MIG and store them in the database.
    
    :param currency_manager: CurrencyManager, currency manager instance
    :param db: OrthancDB, database instance
    :param logger: logging.Logger, logger instance
    :return: bool, True if successful, False otherwise
    """
    try:
        # Fetch rates from MIG
        logger.info("Fetching exchange rates from mig.kz")
        rates = currency_manager.fetch_mig_exchange_rates()

        if not rates:
            logger.error("No exchange rates fetched from mig.kz")
            return False

        logger.info(f"Fetched rates: {rates}")

        # Store rates in database
        stored_count = 0
        fetch_time = datetime.now(timezone.utc)

        for currency, rate in rates.items():
            try:
                success = db.insert_exchange_rate(currency, rate, fetch_time)
                if success:
                    stored_count += 1
                    logger.info(f"Stored {currency} rate: {rate} KZT")
                else:
                    logger.error(f"Failed to store {currency} rate")
            except Exception as e:
                logger.error(f"Error storing {currency} rate: {e}")

        if stored_count > 0:
            logger.info(f"Successfully stored {stored_count} exchange rates")
            return True
        else:
            logger.error("No rates were stored in database")
            return False

    except Exception as e:
        logger.error(f"Error in fetch_and_store_market_data: {e}")
        return False


def get_market_data_status(db_path: str = "flats.db") -> Dict:
    """
    Get status of market data service and latest rates.
    
    :param db_path: str, path to SQLite database file
    :return: Dict, status information
    """
    try:
        db = OrthancDB(db_path)

        # Get latest rates
        latest_rates = {}
        currencies = ['USD', 'EUR', 'GBP']

        for currency in currencies:
            rate = db.get_latest_rate(currency)
            if rate:
                latest_rates[currency] = rate

        # Get all currencies in database
        all_currencies = db.get_all_currencies()

        # Get rates by date range (last 7 days)
        end_date = datetime.now().strftime('%Y-%m-%d')
        start_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')

        recent_rates = {}
        for currency in currencies:
            rates = db.get_rates_by_date_range(currency, start_date, end_date)
            recent_rates[currency] = len(rates)

        return {
            'latest_rates': latest_rates,
            'all_currencies': all_currencies,
            'recent_rates_count': recent_rates,
            'status': 'active' if latest_rates else 'inactive'
        }

    except Exception as e:
        return {
            'error': str(e),
            'status': 'error'
        }


if __name__ == "__main__":
    """
    Launch the real-time market data service.
    """
    import argparse

    parser = argparse.ArgumentParser(description='Launch real-time market data service')
    parser.add_argument('--db-path', default='flats.db', help='Database file path')
    parser.add_argument('--log-level', type=int, default=logging.INFO, help='Logging level')
    parser.add_argument('--status', action='store_true', help='Show market data status and exit')

    args = parser.parse_args()

    if args.status:
        # Show status and exit
        status = get_market_data_status(args.db_path)
        print("Market Data Status:")
        print(f"  Status: {status.get('status', 'unknown')}")
        print(f"  Latest rates: {status.get('latest_rates', {})}")
        print(f"  All currencies: {status.get('all_currencies', [])}")
        print(f"  Recent rates count: {status.get('recent_rates_count', {})}")
        if 'error' in status:
            print(f"  Error: {status['error']}")
    else:
        # Launch the service
        launch_realtime_market_data(args.db_path, args.log_level)
