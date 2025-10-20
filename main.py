
"""
Main entry point for Orthanc Capital Krisha.kz Scraper.

This script provides access to all the main functionality:
- Scraping individual flats
- Running scheduled scraping jobs
- Analyzing residential complexes
- Database management
"""
import logging
import sys
import argparse
import re
from pathlib import Path

# Add the module src directories to Python path
sys.path.insert(0, str(Path(__file__).parent / "cli" / "src"))
sys.path.insert(0, str(Path(__file__).parent / "analytics" / "src"))
sys.path.insert(0, str(Path(__file__).parent / "db" / "src"))
sys.path.insert(0, str(Path(__file__).parent / "scrapers" / "src"))

from scheduler import ScraperScheduler
from jk_analytics import JKAnalytics
from db.src.write_read_database import OrthancDB
from complex_scraper import update_complex_database
from search_scraper import scrape_and_save_search_results
import toml


def load_recommendation_thresholds(config_path: str = "config/src/config.toml") -> dict:
    """
    Load recommendation thresholds from config file.
    
    :param config_path: str, path to config file
    :return: dict, recommendation thresholds
    """
    try:
        with open(config_path, 'r') as f:
            config = toml.load(f)
        
        recommendations = config.get('recommendations', {})
        return {
            'strong_buy_yield': recommendations.get('strong_buy_yield', 20.0),
            'buy_yield': recommendations.get('buy_yield', 8.0),
            'consider_yield': recommendations.get('consider_yield', 5.0),
            'excellent_deal_discount': recommendations.get('excellent_deal_discount', -15.0),
            'good_deal_discount': recommendations.get('good_deal_discount', -5.0),
            'fair_deal_discount': recommendations.get('fair_deal_discount', 5.0)
        }
    except Exception as e:
        logging.info(f"Warning: Could not load recommendation thresholds: {e}")
        # Return default values
        return {
            'strong_buy_yield': 20.0,
            'buy_yield': 8.0,
            'consider_yield': 5.0,
            'excellent_deal_discount': -15.0,
            'good_deal_discount': -5.0,
            'fair_deal_discount': 5.0
        }


def load_analysis_config(config_path: str = "config/src/config.toml") -> dict:
    """
    Load analysis configuration from config file.
    
    :param config_path: str, path to config file
    :return: dict, analysis configuration
    """
    try:
        with open(config_path, 'r') as f:
            config = toml.load(f)
        
        analysis = config.get('analysis', {})
        return {
            'default_area_tolerance': analysis.get('default_area_tolerance', 10.0)
        }
    except Exception as e:
        logging.info(f"Warning: Could not load analysis config: {e}")
        # Return default values
        return {
            'default_area_tolerance': 10.0
        }


def scrape_complex_data(complex_name: str, complex_id: str = None) -> bool:
    """
    Automatically scrape rental and sales data for a complex.
    
    :param complex_name: str, name of the complex
    :param complex_id: str, complex ID (optional)
    :return: bool, True if scraping was successful
    """
    try:
        logging.info(f"Auto-scraping data for {complex_name}...")
        
        # Construct search URLs for rental and sales
        rental_url = f"https://krisha.kz/arenda/kvartiry/almaty/?das[map.complex]={complex_id}" if complex_id else f"https://krisha.kz/arenda/kvartiry/almaty/?das[live.square][to]=35"
        sales_url = f"https://krisha.kz/prodazha/kvartiry/almaty/?das[map.complex]={complex_id}" if complex_id else f"https://krisha.kz/prodazha/kvartiry/almaty/?das[live.square][to]=35"
        
        # Scrape rental data
        logging.info(f"   Scraping rental data...")
        rental_flats = scrape_and_save_search_results(rental_url, max_flats=20, delay=1.0)
        
        # Scrape sales data
        logging.info(f"   Scraping sales data...")
        sales_flats = scrape_and_save_search_results(sales_url, max_flats=20, delay=1.0)
        
        total_scraped = len(rental_flats) + len(sales_flats)
        logging.info(f"Successfully scraped {total_scraped} flats for {complex_name}")
        
        return total_scraped > 0
        
    except Exception as e:
        logging.info(f"Error scraping data for {complex_name}: {e}")
        return False


def main():
    """
    Main entry point with command line interface.
    """
    parser = argparse.ArgumentParser(
        description='Orthanc Capital - Krisha.kz Scraper',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run all configured scraping jobs
  python main.py schedule --all

  # Run specific rental query
  python main.py schedule --query-type rental --query-name "Meridian Apartments - 1 room rentals"

  # Analyze a residential complex
  python main.py analyze --complex "Meridian" --area-max 35

  # Update residential complex database
  python main.py update-complexes

  # Show database statistics
  python main.py stats

  # Search database
  python main.py search --min-price 300000 --max-price 600000

  # Estimate investment potential for a flat
  python main.py estimate --flat-id 1003924251
        """
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # Schedule command
    schedule_parser = subparsers.add_parser('schedule', help='Run scraping jobs')
    schedule_parser.add_argument('--all', action='store_true', help='Run all configured queries')
    schedule_parser.add_argument('--query-type', choices=['rental', 'sales'], help='Run specific query type')
    schedule_parser.add_argument('--query-name', help='Run specific query by name')
    schedule_parser.add_argument('--config', default='config/config.toml', help='Configuration file path')
    
    # Analyze command
    analyze_parser = subparsers.add_parser('analyze', help='Analyze residential complex data')
    analyze_parser.add_argument('--complex', required=True, help='Complex name to analyze')
    analyze_parser.add_argument('--area-max', type=float, default=35.0, help='Maximum area in m²')
    analyze_parser.add_argument('--date', help='Query date (YYYY-MM-DD), defaults to latest')
    
    # Update complexes command
    update_parser = subparsers.add_parser('update-complexes', help='Update residential complex database')
    
    # Stats command
    stats_parser = subparsers.add_parser('stats', help='Show database statistics')
    stats_parser.add_argument('--days', type=int, default=30, help='Number of days to analyze')
    
    # Search command
    search_parser = subparsers.add_parser('search', help='Search database')
    search_parser.add_argument('--min-price', type=int, help='Minimum price')
    search_parser.add_argument('--max-price', type=int, help='Maximum price')
    search_parser.add_argument('--complex', help='Residential complex name')
    search_parser.add_argument('--type', choices=['rental', 'sales'], help='Flat type')
    
    # Estimate command
    estimate_parser = subparsers.add_parser('estimate', help='Estimate investment potential for a specific flat')
    estimate_parser.add_argument('--flat-id', required=True, help='Flat ID to analyze')
    estimate_parser.add_argument('--config', default='config/config.toml', help='Configuration file path')
    
    # Load default area tolerance from config for the estimate command
    try:
        analysis_config = load_analysis_config('config/config.toml')
        default_area_tolerance = analysis_config['default_area_tolerance']
    except:
        default_area_tolerance = 10.0
    
    estimate_parser.add_argument('--area-tolerance', type=float, default=default_area_tolerance, 
                                help=f'Area tolerance percentage (default: {default_area_tolerance}%%)')
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return
    
    try:
        if args.command == 'schedule':
            handle_schedule(args)
        elif args.command == 'analyze':
            handle_analyze(args)
        elif args.command == 'update-complexes':
            handle_update_complexes()
        elif args.command == 'stats':
            handle_stats(args)
        elif args.command == 'search':
            handle_search(args)
        elif args.command == 'estimate':
            handle_estimate(args)
        else:
            logging.info(f"Unknown command: {args.command}")
            parser.print_help()
    
    except Exception as e:
        logging.info(f"Error: {e}")
        sys.exit(1)


def handle_schedule(args):
    """Handle schedule command."""
    scheduler = ScraperScheduler(args.config)
    
    if args.query_name and args.query_type:
        # Run single query
        scraped_count = scheduler.run_single_query(args.query_type, args.query_name)
        logging.info(f"Scraped {scraped_count} flats from query '{args.query_name}'")
    
    elif args.all:
        # Run all queries
        summary = scheduler.run_all_queries()
        logging.info(f"\nSummary:")
        logging.info(f"   Total flats scraped: {summary['total_flats']}")
        logging.info(f"   Rental flats: {summary['total_rental_flats']}")
        logging.info(f"   Sales flats: {summary['total_sales_flats']}")
        logging.info(f"   Duration: {summary['duration_seconds']:.2f} seconds")
    
    else:
        # Show available queries
        logging.info("Available queries:")
        logging.info("\nRental queries:")
        for query in scheduler.config.get('rental_queries', []):
            logging.info(f"   - {query['name']}")
        
        logging.info("\nSales queries:")
        for query in scheduler.config.get('sales_queries', []):
            logging.info(f"   - {query['name']}")


def handle_analyze(args):
    """Handle analyze command."""
    analytics = JKAnalytics()
    analytics.print_jk_analysis(args.complex, args.area_max, args.date)


def handle_update_complexes():
    """Handle update-complexes command."""
    logging.info("Updating residential complex database...")
    count = update_complex_database()
    logging.info(f"Updated {count} residential complexes")


def handle_stats(args):
    """Handle stats command."""
    from datetime import datetime, timedelta
    
    db = OrthancDB()
    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=args.days)).strftime('%Y-%m-%d')
    
    stats = db.get_historical_statistics(start_date, end_date)
    
    logging.info(f"Database Statistics ({args.days} days):")
    logging.info(f"   Date range: {start_date} to {end_date}")
    logging.info(f"   Total rentals: {stats['rental_stats']['total_rentals']}")
    logging.info(f"   Total sales: {stats['sales_stats']['total_sales']}")
    
    if stats['rental_stats']['total_rentals'] > 0:
        logging.info(f"   Rental price range: {stats['rental_stats']['min_rental_price']:,} - {stats['rental_stats']['max_rental_price']:,} tenge")
    
    if stats['sales_stats']['total_sales'] > 0:
        logging.info(f"   Sales price range: {stats['sales_stats']['min_sales_price']:,} - {stats['sales_stats']['max_sales_price']:,} tenge")


def handle_search(args):
    """Handle search command."""
    db = OrthancDB()
    
    # Build search criteria
    criteria = {}
    if args.min_price:
        criteria['min_price'] = args.min_price
    if args.max_price:
        criteria['max_price'] = args.max_price
    if args.complex:
        criteria['residential_complex'] = args.complex
    if args.type:
        criteria['type'] = args.type
    
    # For now, just show that search is available
    logging.info("Search functionality available")
    logging.info("   Use the CLI tools directly for advanced search:")
    logging.info("   python cli/cli_tool.py search --min-price 300000 --max-price 600000")


def handle_estimate(args):
    """Handle estimate command."""
    from scrapers.src.krisha_scraper import scrape_flat_info
    from db.src.write_read_database import save_sales_flat_to_db
    from datetime import datetime
    import statistics
    
    logging.info(f"Analyzing flat {args.flat_id} for investment potential...")
    
    # 1. Scrape and save flat info to DB
    flat_url = f"https://krisha.kz/a/show/{args.flat_id}"
    logging.info(f"Scraping flat info from: {flat_url}")
    
    try:
        flat_info = scrape_flat_info(flat_url)
        query_date = datetime.now().strftime('%Y-%m-%d')
        
        # Save to sales database (assuming it's a sale listing)
        success = save_sales_flat_to_db(flat_info, flat_url, query_date)
        
        if success:
            logging.info(f"Flat info saved to database")
        else:
            logging.info(f"Flat info already exists in database")
        
        logging.info(f"Flat Details:")
        logging.info(f"   Price: {flat_info.price:,} tenge")
        logging.info(f"   Area: {flat_info.area} m²")
        logging.info(f"   Residential Complex: {flat_info.residential_complex or 'N/A'}")
        logging.info(f"   Floor: {flat_info.floor or 'N/A'}")
        logging.info(f"   Construction Year: {flat_info.construction_year or 'N/A'}")
        
    except Exception as e:
        logging.info(f"Error scraping flat: {e}")
        return
    
    # 2. Find similar rentals
    logging.info(f"\nFinding similar rental flats...")
    db = OrthancDB()
    db.connect()
    
    try:
        # Calculate area range
        area_min = flat_info.area * (1 - args.area_tolerance / 100)
        area_max = flat_info.area * (1 + args.area_tolerance / 100)
        
        # Extract room count from description or title
        room_count = extract_room_count(flat_info.description or "")
        
        # Query similar rentals - use DISTINCT to avoid duplicates and get historical data
        cursor = db.conn.execute("""
            SELECT DISTINCT flat_id, price, area, residential_complex, floor, construction_year
            FROM rental_flats 
            WHERE residential_complex LIKE ? 
            AND area BETWEEN ? AND ?
            ORDER BY flat_id, query_date DESC
        """, (f'%{flat_info.residential_complex}%' if flat_info.residential_complex else '%', 
              area_min, area_max))
        
        # Group by flat_id to get only the most recent entry for each flat
        rental_data = {}
        for row in cursor.fetchall():
            flat_id = row[0]
            if flat_id not in rental_data:
                rental_data[flat_id] = row[1:]  # Store price, area, residential_complex, floor, construction_year
        
        similar_rentals = list(rental_data.values())
        
        logging.info(f"   Found {len(similar_rentals)} unique similar rental flats")
        
        if similar_rentals:
            rental_prices = [r[0] for r in similar_rentals]
            rental_areas = [r[1] for r in similar_rentals]
            
            rental_stats = {
                'count': len(similar_rentals),
                'min_price': min(rental_prices),
                'max_price': max(rental_prices),
                'avg_price': sum(rental_prices) / len(rental_prices),
                'median_price': statistics.median(rental_prices),
                'avg_area': sum(rental_areas) / len(rental_areas)
            }
            
            logging.info(f"   Rental price range: {rental_stats['min_price']:,} - {rental_stats['max_price']:,} tenge")
            logging.info(f"   Average rental price: {rental_stats['avg_price']:,.0f} tenge")
            logging.info(f"   Median rental price: {rental_stats['median_price']:,.0f} tenge")
        
        # 3. Find similar sales
        logging.info(f"\n💰 Finding similar sales flats...")
        
        cursor = db.conn.execute("""
            SELECT DISTINCT flat_id, price, area, residential_complex, floor, construction_year
            FROM sales_flats 
            WHERE residential_complex LIKE ? 
            AND area BETWEEN ? AND ?
            ORDER BY flat_id, query_date DESC
        """, (f'%{flat_info.residential_complex}%' if flat_info.residential_complex else '%', 
              area_min, area_max))
        
        # Group by flat_id to get only the most recent entry for each flat
        sales_data = {}
        for row in cursor.fetchall():
            flat_id = row[0]
            if flat_id not in sales_data:
                sales_data[flat_id] = row[1:]  # Store price, area, residential_complex, floor, construction_year
        
        similar_sales = list(sales_data.values())
        
        logging.info(f"   Found {len(similar_sales)} unique similar sales flats")
        
        if similar_sales:
            sales_prices = [s[0] for s in similar_sales]
            sales_areas = [s[1] for s in similar_sales]
            
            sales_stats = {
                'count': len(similar_sales),
                'min_price': min(sales_prices),
                'max_price': max(sales_prices),
                'avg_price': sum(sales_prices) / len(sales_prices),
                'median_price': statistics.median(sales_prices),
                'avg_area': sum(sales_areas) / len(sales_areas)
            }
            
            logging.info(f"   Sales price range: {sales_stats['min_price']:,} - {sales_stats['max_price']:,} tenge")
            logging.info(f"   Average sales price: {sales_stats['avg_price']:,.0f} tenge")
            logging.info(f"   Median sales price: {sales_stats['median_price']:,.0f} tenge")
        
        # 4. Calculate investment insights
        logging.info(f"\n📈 Investment Analysis:")
        logging.info("=" * 50)
        
        # If insufficient data, try to scrape more data for the complex
        if (not similar_rentals or len(similar_rentals) < 5) or (not similar_sales or len(similar_sales) < 5):
            logging.info(f"Insufficient data for analysis. Attempting to scrape more data for {flat_info.residential_complex or 'this complex'}...")
            
            # Try to find complex ID
            from scrapers.complex_scraper import search_complex_by_name
            complex_info = search_complex_by_name(flat_info.residential_complex or '')
            complex_id = complex_info.get('complex_id') if complex_info else None
            
            # Scrape data for this complex
            if scrape_complex_data(flat_info.residential_complex or 'Unknown', complex_id):
                logging.info(f"Successfully scraped additional data. Re-analyzing...")
                
                # Re-query similar properties
                cursor = db.conn.execute("""
                    SELECT DISTINCT flat_id, price, area, residential_complex, floor, construction_year
                    FROM rental_flats 
                    WHERE residential_complex LIKE ? 
                    AND area BETWEEN ? AND ?
                    ORDER BY flat_id, query_date DESC
                """, (f'%{flat_info.residential_complex}%' if flat_info.residential_complex else '%', 
                      area_min, area_max))
                
                rental_data = {}
                for row in cursor.fetchall():
                    flat_id = row[0]
                    if flat_id not in rental_data:
                        rental_data[flat_id] = row[1:]
                
                similar_rentals = list(rental_data.values())
                
                cursor = db.conn.execute("""
                    SELECT DISTINCT flat_id, price, area, residential_complex, floor, construction_year
                    FROM sales_flats 
                    WHERE residential_complex LIKE ? 
                    AND area BETWEEN ? AND ?
                    ORDER BY flat_id, query_date DESC
                """, (f'%{flat_info.residential_complex}%' if flat_info.residential_complex else '%', 
                      area_min, area_max))
                
                sales_data = {}
                for row in cursor.fetchall():
                    flat_id = row[0]
                    if flat_id not in sales_data:
                        sales_data[flat_id] = row[1:]
                
                similar_sales = list(sales_data.values())
                
                # Recalculate statistics
                if similar_rentals:
                    rental_prices = [r[0] for r in similar_rentals]
                    rental_areas = [r[1] for r in similar_rentals]
                    rental_stats = {
                        'count': len(similar_rentals),
                        'min_price': min(rental_prices),
                        'max_price': max(rental_prices),
                        'avg_price': sum(rental_prices) / len(rental_prices),
                        'median_price': statistics.median(rental_prices),
                        'avg_area': sum(rental_areas) / len(rental_areas)
                    }
                else:
                    rental_stats = None
                
                if similar_sales:
                    sales_prices = [s[0] for s in similar_sales]
                    sales_areas = [s[1] for s in similar_sales]
                    sales_stats = {
                        'count': len(similar_sales),
                        'min_price': min(sales_prices),
                        'max_price': max(sales_prices),
                        'avg_price': sum(sales_prices) / len(sales_prices),
                        'median_price': statistics.median(sales_prices),
                        'avg_area': sum(sales_areas) / len(sales_areas)
                    }
                else:
                    sales_stats = None
                
                logging.info(f"   Found {len(similar_rentals)} rental flats and {len(similar_sales)} sales flats after scraping")
            else:
                logging.info(f"Failed to scrape additional data")
        
        if similar_rentals and similar_sales:
            # Load recommendation thresholds
            thresholds = load_recommendation_thresholds()
            
            # Calculate rental yield
            median_rental = rental_stats['median_price']
            median_sales = sales_stats['median_price']
            current_price = flat_info.price
            
            annual_rental_income = median_rental * 12
            rental_yield = (annual_rental_income / current_price) * 100
            
            # Price comparison
            price_vs_median = ((current_price - median_sales) / median_sales) * 100
            price_vs_avg = ((current_price - sales_stats['avg_price']) / sales_stats['avg_price']) * 100
            
            logging.info(f"💡 Investment Potential:")
            logging.info(f"   Expected annual rental income: {annual_rental_income:,.0f} tenge")
            logging.info(f"   Rental yield: {rental_yield:.2f}%")
            
            logging.info(f"\n💰 Price Analysis:")
            logging.info(f"   Your price: {current_price:,} tenge")
            logging.info(f"   Median similar sales: {median_sales:,} tenge")
            logging.info(f"   Average similar sales: {sales_stats['avg_price']:,.0f} tenge")
            
            # Price rating based on configurable thresholds
            if price_vs_median < thresholds['excellent_deal_discount']:
                price_rating = "🔥 Excellent deal (significantly below median)"
            elif price_vs_median < thresholds['good_deal_discount']:
                price_rating = "Good deal (below median)"
            elif price_vs_median < thresholds['fair_deal_discount']:
                price_rating = "⚖️ Fair price (around median)"
            else:
                price_rating = "Expensive (above median)"
            
            logging.info(f"   Price vs median: {price_vs_median:+.1f}% ({price_rating})")
            
            # Yield rating based on configurable thresholds
            if rental_yield > thresholds['strong_buy_yield']:
                yield_rating = f"🚀 Excellent yield (>{thresholds['strong_buy_yield']}%)"
            elif rental_yield > thresholds['buy_yield']:
                yield_rating = f"Good yield ({thresholds['buy_yield']}-{thresholds['strong_buy_yield']}%)"
            elif rental_yield > thresholds['consider_yield']:
                yield_rating = f"⚖️ Moderate yield ({thresholds['consider_yield']}-{thresholds['buy_yield']}%)"
            else:
                yield_rating = f"Low yield (<{thresholds['consider_yield']}%)"
            
            logging.info(f"   Yield rating: {yield_rating}")
            
            # Overall recommendation based on configurable thresholds
            logging.info(f"\n🎯 Overall Recommendation:")
            if rental_yield > thresholds['strong_buy_yield']:
                logging.info(f"   🚀 STRONG BUY - Excellent yield (>{thresholds['strong_buy_yield']}%)")
            elif rental_yield > thresholds['buy_yield'] and price_vs_median < 0:
                logging.info(f"   BUY - Good yield (>{thresholds['buy_yield']}%) + good price")
            elif rental_yield > thresholds['consider_yield'] and price_vs_median < thresholds['fair_deal_discount']:
                logging.info(f"   ⚖️ CONSIDER - Moderate potential (>{thresholds['consider_yield']}% yield)")
            else:
                logging.info(f"   PASS - Low investment potential (<{thresholds['consider_yield']}% yield)")
            
            # Discount-based return analysis
            logging.info(f"\n💰 Discount Analysis:")
            logging.info("=" * 30)
            
            for discount in [10, 20]:
                discounted_price = current_price * (1 - discount / 100)
                discounted_yield = (annual_rental_income / discounted_price) * 100
                savings = current_price - discounted_price
                
                logging.info(f"\n📉 {discount}% Discount Scenario:")
                logging.info(f"   Discounted price: {discounted_price:,.0f} tenge")
                logging.info(f"   Savings: {savings:,.0f} tenge")
                logging.info(f"   Rental yield: {discounted_yield:.2f}%")
                
                # Yield rating for discount scenario
                if discounted_yield > thresholds['strong_buy_yield']:
                    yield_rating = f"🚀 Excellent yield (>{thresholds['strong_buy_yield']}%)"
                elif discounted_yield > thresholds['buy_yield']:
                    yield_rating = f"🔥 Very good yield ({thresholds['buy_yield']}-{thresholds['strong_buy_yield']}%)"
                elif discounted_yield > thresholds['consider_yield']:
                    yield_rating = f"Good yield ({thresholds['consider_yield']}-{thresholds['buy_yield']}%)"
                else:
                    yield_rating = f"Low yield (<{thresholds['consider_yield']}%)"
                
                logging.info(f"   Yield rating: {yield_rating}")
                
                # Price comparison with discount
                price_vs_median_discounted = ((discounted_price - median_sales) / median_sales) * 100
                if price_vs_median_discounted < thresholds['excellent_deal_discount']:
                    price_rating = "🔥 Excellent deal (significantly below median)"
                elif price_vs_median_discounted < thresholds['good_deal_discount']:
                    price_rating = "Very good deal (well below median)"
                elif price_vs_median_discounted < thresholds['fair_deal_discount']:
                    price_rating = "⚖️ Good deal (below median)"
                else:
                    price_rating = "Above median price"
                
                logging.info(f"   Price vs median: {price_vs_median_discounted:+.1f}% ({price_rating})")
                
                # Recommendation for this discount level
                if discounted_yield > thresholds['strong_buy_yield'] and price_vs_median_discounted < thresholds['excellent_deal_discount']:
                    logging.info(f"   💡 Recommendation: 🚀 STRONG BUY with {discount}% discount")
                elif discounted_yield > thresholds['buy_yield'] and price_vs_median_discounted < thresholds['good_deal_discount']:
                    logging.info(f"   💡 Recommendation: BUY with {discount}% discount")
                elif discounted_yield > thresholds['consider_yield']:
                    logging.info(f"   💡 Recommendation: ⚖️ CONSIDER with {discount}% discount")
                else:
                    logging.info(f"   💡 Recommendation: PASS even with {discount}% discount")
                
        else:
            logging.info("   Insufficient data for analysis")
            if not similar_rentals:
                logging.info("   - No similar rental data found")
            if not similar_sales:
                logging.info("   - No similar sales data found")
    
    finally:
        db.disconnect()


def extract_room_count(description: str) -> int:
    """
    Extract room count from description.
    
    :param description: str, flat description
    :return: int, number of rooms (default: 1)
    """
    if not description:
        return 1
    
    # Look for room indicators in description
    room_patterns = [
        r'(\d+)\s*комнат',
        r'(\d+)\s*комн',
        r'(\d+)\s*room',
        r'(\d+)\s*квартир',
    ]
    
    for pattern in room_patterns:
        match = re.search(pattern, description, re.IGNORECASE)
        if match:
            return int(match.group(1))
    
    return 1


if __name__ == "__main__":
    main() 