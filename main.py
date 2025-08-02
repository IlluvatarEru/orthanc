#!/usr/bin/env python3
"""
Main entry point for Orthanc Capital Krisha.kz Scraper.

This script provides access to all the main functionality:
- Scraping individual flats
- Running scheduled scraping jobs
- Analyzing residential complexes
- Database management
"""

import sys
import argparse
import re
from pathlib import Path

# Add the project root to Python path
sys.path.insert(0, str(Path(__file__).parent))

from cli.scheduler import ScraperScheduler
from analytics.jk_analytics import JKAnalytics
from db.enhanced_database import EnhancedFlatDatabase
from scrapers.complex_scraper import update_complex_database


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
    estimate_parser.add_argument('--area-tolerance', type=float, default=25.0, help='Area tolerance percentage (default: 25%)')
    estimate_parser.add_argument('--config', default='config/config.toml', help='Configuration file path')
    
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
            print(f"Unknown command: {args.command}")
            parser.print_help()
    
    except Exception as e:
        print(f"❌ Error: {e}")
        sys.exit(1)


def handle_schedule(args):
    """Handle schedule command."""
    scheduler = ScraperScheduler(args.config)
    
    if args.query_name and args.query_type:
        # Run single query
        scraped_count = scheduler.run_single_query(args.query_type, args.query_name)
        print(f"✅ Scraped {scraped_count} flats from query '{args.query_name}'")
    
    elif args.all:
        # Run all queries
        summary = scheduler.run_all_queries()
        print(f"\n📊 Summary:")
        print(f"   Total flats scraped: {summary['total_flats']}")
        print(f"   Rental flats: {summary['total_rental_flats']}")
        print(f"   Sales flats: {summary['total_sales_flats']}")
        print(f"   Duration: {summary['duration_seconds']:.2f} seconds")
    
    else:
        # Show available queries
        print("Available queries:")
        print("\nRental queries:")
        for query in scheduler.config.get('rental_queries', []):
            print(f"   - {query['name']}")
        
        print("\nSales queries:")
        for query in scheduler.config.get('sales_queries', []):
            print(f"   - {query['name']}")


def handle_analyze(args):
    """Handle analyze command."""
    analytics = JKAnalytics()
    analytics.print_jk_analysis(args.complex, args.area_max, args.date)


def handle_update_complexes():
    """Handle update-complexes command."""
    print("🔄 Updating residential complex database...")
    count = update_complex_database()
    print(f"✅ Updated {count} residential complexes")


def handle_stats(args):
    """Handle stats command."""
    from datetime import datetime, timedelta
    
    db = EnhancedFlatDatabase()
    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=args.days)).strftime('%Y-%m-%d')
    
    stats = db.get_historical_statistics(start_date, end_date)
    
    print(f"📊 Database Statistics ({args.days} days):")
    print(f"   Date range: {start_date} to {end_date}")
    print(f"   Total rentals: {stats['rental_stats']['total_rentals']}")
    print(f"   Total sales: {stats['sales_stats']['total_sales']}")
    
    if stats['rental_stats']['total_rentals'] > 0:
        print(f"   Rental price range: {stats['rental_stats']['min_rental_price']:,} - {stats['rental_stats']['max_rental_price']:,} tenge")
    
    if stats['sales_stats']['total_sales'] > 0:
        print(f"   Sales price range: {stats['sales_stats']['min_sales_price']:,} - {stats['sales_stats']['max_sales_price']:,} tenge")


def handle_search(args):
    """Handle search command."""
    db = EnhancedFlatDatabase()
    
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
    print("🔍 Search functionality available")
    print("   Use the CLI tools directly for advanced search:")
    print("   python cli/cli_tool.py search --min-price 300000 --max-price 600000")


def handle_estimate(args):
    """Handle estimate command."""
    from common.krisha_scraper import scrape_flat_info
    from db.enhanced_database import save_sales_flat_to_db
    from datetime import datetime
    import statistics
    
    print(f"🏠 Analyzing flat {args.flat_id} for investment potential...")
    
    # 1. Scrape and save flat info to DB
    flat_url = f"https://krisha.kz/a/show/{args.flat_id}"
    print(f"📥 Scraping flat info from: {flat_url}")
    
    try:
        flat_info = scrape_flat_info(flat_url)
        query_date = datetime.now().strftime('%Y-%m-%d')
        
        # Save to sales database (assuming it's a sale listing)
        success = save_sales_flat_to_db(flat_info, flat_url, query_date)
        
        if success:
            print(f"✅ Flat info saved to database")
        else:
            print(f"⚠️ Flat info already exists in database")
        
        print(f"📊 Flat Details:")
        print(f"   Price: {flat_info.price:,} tenge")
        print(f"   Area: {flat_info.area} m²")
        print(f"   Residential Complex: {flat_info.residential_complex or 'N/A'}")
        print(f"   Floor: {flat_info.floor or 'N/A'}")
        print(f"   Construction Year: {flat_info.construction_year or 'N/A'}")
        
    except Exception as e:
        print(f"❌ Error scraping flat: {e}")
        return
    
    # 2. Find similar rentals
    print(f"\n🔍 Finding similar rental flats...")
    db = EnhancedFlatDatabase()
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
        
        print(f"   Found {len(similar_rentals)} unique similar rental flats")
        
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
            
            print(f"   Rental price range: {rental_stats['min_price']:,} - {rental_stats['max_price']:,} tenge")
            print(f"   Average rental price: {rental_stats['avg_price']:,.0f} tenge")
            print(f"   Median rental price: {rental_stats['median_price']:,.0f} tenge")
        
        # 3. Find similar sales
        print(f"\n💰 Finding similar sales flats...")
        
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
        
        print(f"   Found {len(similar_sales)} unique similar sales flats")
        
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
            
            print(f"   Sales price range: {sales_stats['min_price']:,} - {sales_stats['max_price']:,} tenge")
            print(f"   Average sales price: {sales_stats['avg_price']:,.0f} tenge")
            print(f"   Median sales price: {sales_stats['median_price']:,.0f} tenge")
        
        # 4. Calculate investment insights
        print(f"\n📈 Investment Analysis:")
        print("=" * 50)
        
        if similar_rentals and similar_sales:
            # Calculate rental yield
            median_rental = rental_stats['median_price']
            median_sales = sales_stats['median_price']
            current_price = flat_info.price
            
            annual_rental_income = median_rental * 12
            rental_yield = (annual_rental_income / current_price) * 100
            
            # Price comparison
            price_vs_median = ((current_price - median_sales) / median_sales) * 100
            price_vs_avg = ((current_price - sales_stats['avg_price']) / sales_stats['avg_price']) * 100
            
            print(f"💡 Investment Potential:")
            print(f"   Expected annual rental income: {annual_rental_income:,.0f} tenge")
            print(f"   Rental yield: {rental_yield:.2f}%")
            
            print(f"\n💰 Price Analysis:")
            print(f"   Your price: {current_price:,} tenge")
            print(f"   Median similar sales: {median_sales:,} tenge")
            print(f"   Average similar sales: {sales_stats['avg_price']:,.0f} tenge")
            
            if price_vs_median < -10:
                price_rating = "🔥 Excellent deal (significantly below median)"
            elif price_vs_median < -5:
                price_rating = "✅ Good deal (below median)"
            elif price_vs_median < 5:
                price_rating = "⚖️ Fair price (around median)"
            elif price_vs_median < 10:
                price_rating = "⚠️ Above median price"
            else:
                price_rating = "❌ Expensive (significantly above median)"
            
            print(f"   Price vs median: {price_vs_median:+.1f}% ({price_rating})")
            
            if rental_yield > 8:
                yield_rating = "🔥 Excellent yield (>8%)"
            elif rental_yield > 6:
                yield_rating = "✅ Good yield (6-8%)"
            elif rental_yield > 4:
                yield_rating = "⚖️ Moderate yield (4-6%)"
            else:
                yield_rating = "❌ Low yield (<4%)"
            
            print(f"   Yield rating: {yield_rating}")
            
            # Overall recommendation
            print(f"\n🎯 Overall Recommendation:")
            if rental_yield > 20:
                print("   🚀 STRONG BUY - Excellent yield (>20%)")
            elif rental_yield > 6 and price_vs_median < 0:
                print("   ✅ BUY - Good yield + good price")
            elif rental_yield > 5 and price_vs_median < 5:
                print("   ⚖️ CONSIDER - Moderate potential")
            else:
                print("   ❌ PASS - Low investment potential")
            
            # Discount-based return analysis
            print(f"\n💰 Discount Analysis:")
            print("=" * 30)
            
            for discount in [10, 20]:
                discounted_price = current_price * (1 - discount / 100)
                discounted_yield = (annual_rental_income / discounted_price) * 100
                savings = current_price - discounted_price
                
                print(f"\n📉 {discount}% Discount Scenario:")
                print(f"   Discounted price: {discounted_price:,.0f} tenge")
                print(f"   Savings: {savings:,.0f} tenge")
                print(f"   Rental yield: {discounted_yield:.2f}%")
                
                if discounted_yield > 20:
                    yield_rating = "🚀 Excellent yield (>20%)"
                elif discounted_yield > 15:
                    yield_rating = "🔥 Very good yield (15-20%)"
                elif discounted_yield > 10:
                    yield_rating = "✅ Good yield (10-15%)"
                elif discounted_yield > 6:
                    yield_rating = "⚖️ Moderate yield (6-10%)"
                else:
                    yield_rating = "❌ Low yield (<6%)"
                
                print(f"   Yield rating: {yield_rating}")
                
                # Price comparison with discount
                price_vs_median_discounted = ((discounted_price - median_sales) / median_sales) * 100
                if price_vs_median_discounted < -15:
                    price_rating = "🔥 Excellent deal (significantly below median)"
                elif price_vs_median_discounted < -10:
                    price_rating = "✅ Very good deal (well below median)"
                elif price_vs_median_discounted < -5:
                    price_rating = "⚖️ Good deal (below median)"
                elif price_vs_median_discounted < 0:
                    price_rating = "📊 Fair price (around median)"
                else:
                    price_rating = "⚠️ Above median price"
                
                print(f"   Price vs median: {price_vs_median_discounted:+.1f}% ({price_rating})")
                
                # Recommendation for this discount level
                if discounted_yield > 15 and price_vs_median_discounted < -10:
                    print(f"   💡 Recommendation: 🚀 STRONG BUY with {discount}% discount")
                elif discounted_yield > 10 and price_vs_median_discounted < -5:
                    print(f"   💡 Recommendation: ✅ BUY with {discount}% discount")
                elif discounted_yield > 6:
                    print(f"   💡 Recommendation: ⚖️ CONSIDER with {discount}% discount")
                else:
                    print(f"   💡 Recommendation: ❌ PASS even with {discount}% discount")
                
        else:
            print("   ⚠️ Insufficient data for analysis")
            if not similar_rentals:
                print("   - No similar rental data found")
            if not similar_sales:
                print("   - No similar sales data found")
    
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