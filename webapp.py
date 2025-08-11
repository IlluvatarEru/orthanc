#!/usr/bin/env python3
"""
Web Application for Orthanc Capital Krisha.kz Scraper

Provides a web interface for:
- Searching and analyzing residential complexes (JK)
- Estimating investment potential for individual flats
"""

from flask import Flask, render_template, request, redirect, url_for, flash, jsonify
import sys
from pathlib import Path

# Add the project root to Python path
sys.path.insert(0, str(Path(__file__).parent))

from analytics.jk_analytics import JKAnalytics
from common.krisha_scraper import scrape_flat_info
from db.enhanced_database import save_sales_flat_to_db, EnhancedFlatDatabase
from scrapers.complex_scraper import search_complex_by_name, search_complexes_by_name, search_complexes_by_name_deduplicated, get_all_residential_complexes
from scrapers.search_scraper import scrape_and_save_search_results
from datetime import datetime
import statistics
import toml
from common.krisha_scraper import FlatInfo
from currency import currency_manager, format_price_with_eur
from flask import render_template_string # Added for minimal template test

app = Flask(__name__)
app.secret_key = 'orthanc_capital_2024'  # For flash messages

# Initialize analytics
analytics = JKAnalytics()


def load_recommendation_thresholds(config_path: str = "config/config.toml") -> dict:
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
        print(f"Warning: Could not load recommendation thresholds: {e}")
        # Return default values
        return {
            'strong_buy_yield': 20.0,
            'buy_yield': 8.0,
            'consider_yield': 5.0,
            'excellent_deal_discount': -15.0,
            'good_deal_discount': -5.0,
            'fair_deal_discount': 5.0
        }


def load_analysis_config(config_path: str = "config/config.toml") -> dict:
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
        print(f"Warning: Could not load analysis config: {e}")
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
        print(f"🔄 Auto-scraping data for {complex_name}...")
        
        # Import the pagination-aware scraping functions
        from scrapers.search_scraper import scrape_and_save_search_results_with_pagination
        
        # Construct search URLs for rental and sales
        if complex_id:
            rental_url = f"https://krisha.kz/arenda/kvartiry/almaty/?das[map.complex]={complex_id}"
            sales_url = f"https://krisha.kz/prodazha/kvartiry/almaty/?das[map.complex]={complex_id}"
        else:
            # Fallback to generic search if no complex_id
            rental_url = f"https://krisha.kz/arenda/kvartiry/almaty/?das[live.square][to]=35"
            sales_url = f"https://krisha.kz/prodazha/kvartiry/almaty/?das[live.square][to]=35"
        
                # Scrape rental data with pagination
        print(f"   📥 Scraping rental data...")
        rental_flats = scrape_and_save_search_results_with_pagination(rental_url, max_pages=5, max_flats=None, delay=1.0)

        # Scrape sales data with pagination
        print(f"   📥 Scraping sales data...")
        sales_flats = scrape_and_save_search_results_with_pagination(sales_url, max_pages=5, max_flats=None, delay=1.0)
        
        total_scraped = len(rental_flats) + len(sales_flats)
        print(f"✅ Successfully scraped {total_scraped} flats for {complex_name}")
        
        return total_scraped > 0
        
    except Exception as e:
        print(f"❌ Error scraping data for {complex_name}: {e}")
        return False


@app.route('/')
def index():
    """Dashboard home page."""
    try:
        db = EnhancedFlatDatabase()
        try:
            # Get basic statistics
            rental_count = db.get_flat_count('rental')
            sales_count = db.get_flat_count('sales')
            total_flats = rental_count + sales_count
            complexes_count = db.get_complex_count()
            
            # For demo purposes, show some growth
            new_rentals = max(1, rental_count // 10)
            new_sales = max(1, sales_count // 10)
            new_flats = new_rentals + new_sales
            new_complexes = max(1, complexes_count // 5)
            
            return render_template('index.html',
                                total_flats=total_flats,
                                rental_flats=rental_count,
                                sales_flats=sales_count,
                                complexes=complexes_count,
                                new_flats=new_flats,
                                new_rentals=new_rentals,
                                new_sales=new_sales,
                                new_complexes=new_complexes)
        finally:
            db.disconnect()
    except Exception as e:
        flash(f'Error loading dashboard: {str(e)}', 'error')
        return render_template('index.html',
                            total_flats=0,
                            rental_flats=0,
                            sales_flats=0,
                            complexes=0,
                            new_flats=0,
                            new_rentals=0,
                            new_sales=0,
                            new_complexes=0)


@app.route('/search_jk', methods=['GET', 'POST'])
def search_jk():
    """Search for residential complexes."""
    if request.method == 'POST':
        search_term = request.form.get('complex_name', '').strip()
        if search_term:
            # Search for complexes (with deduplication)
            complexes = search_complexes_by_name_deduplicated(search_term)
            
            # Check if deduplication happened by comparing with non-deduplicated results
            all_complexes = search_complexes_by_name(search_term)
            deduplication_info = None
            if len(all_complexes) > len(complexes):
                deduplication_info = f"Found {len(all_complexes)} results, showing {len(complexes)} unique complexes after removing duplicates."
            
            return render_template('search_jk.html', 
                                complexes=complexes, 
                                search_term=search_term,
                                deduplication_info=deduplication_info)
    
    return render_template('search_jk.html', complexes=[], search_term='')


@app.route('/analyze_jk/<complex_name>')
def analyze_jk(complex_name):
    """Analyze a specific residential complex with unified view."""
    try:
        # Get analysis parameters
        area_max = float(request.args.get('area_max', 1000.0))  # Increased from 100.0 to 1000.0 to remove area filter
        query_date = request.args.get('date', datetime.now().strftime('%Y-%m-%d'))
        
        # Get complex information
        complex_info = search_complex_by_name(complex_name)
        if not complex_info:
            flash(f'Complex "{complex_name}" not found', 'error')
            return redirect(url_for('search_jk'))
        
        # Get all flats for this complex
        db = EnhancedFlatDatabase()
        try:
            # Get rental flats
            rental_flats = db.get_flats_by_complex(complex_name, 'rental')
            
            # Get sales flats
            sales_flats = db.get_flats_by_complex(complex_name, 'sales')
            
            # If no flats found, automatically scrape data
            if not rental_flats and not sales_flats:
                flash(f"No data found for {complex_name}. Automatically fetching latest data from Krisha.kz...", 'info')
                
                # Try to find complex ID
                complex_id = complex_info.get('complex_id') if complex_info else None
                
                # Scrape data for this complex
                if scrape_complex_data(complex_name, complex_id):
                    flash(f"Successfully scraped data for {complex_name}. Analyzing...", 'success')
                    
                    # Get flats again after scraping
                    rental_flats = db.get_flats_by_complex(complex_name, 'rental')
                    sales_flats = db.get_flats_by_complex(complex_name, 'sales')
                else:
                    flash(f"Failed to scrape data for {complex_name}. You can try the 'Refresh Analysis' button later.", 'warning')
            
            # Get comprehensive analysis (this will automatically fetch data if needed)
            analysis = analytics.get_jk_comprehensive_analysis(complex_name, area_max, query_date)
            
            # Get bucket-based analysis for more accurate yield calculation (this will also auto-fetch if needed)
            bucket_analysis = analytics.get_bucket_analysis(complex_name, area_max, query_date)
            
            # Debug logging
            print(f"=== DEBUG: Bucket Analysis for {complex_name} ===")
            print(f"Total buckets: {len(bucket_analysis['bucket_analysis'])}")
            valid_buckets = [b for b in bucket_analysis['bucket_analysis'].values() if b.get('rental_count', 0) > 0 and b.get('sales_count', 0) > 0]
            print(f"Valid buckets: {len(valid_buckets)}")
            if valid_buckets:
                # Filter buckets that have yield analysis
                buckets_with_yield = [b for b in valid_buckets if b.get('yield_analysis') is not None]
                if buckets_with_yield:
                    min_yield = min([b['yield_analysis']['yield_min'] for b in buckets_with_yield])
                    max_yield = max([b['yield_analysis']['yield_max'] for b in buckets_with_yield])
                    print(f"Yield range: {min_yield:.1f}% - {max_yield:.1f}%")
                    for i, bucket in enumerate(buckets_with_yield[:3]):  # Show first 3 buckets
                        print(f"  Bucket {i+1}: {bucket['rooms']}BR {bucket['area_bucket']} - Min: {bucket['yield_analysis']['yield_min']:.1f}%, Max: {bucket['yield_analysis']['yield_max']:.1f}%")
                else:
                    print("No buckets with yield analysis available")
            print("=== END DEBUG ===")
            
            # Check data sufficiency and show appropriate warnings
            rental_count = len(rental_flats)
            sales_count = len(sales_flats)
            analysis_rental_count = analysis.get('rental_stats', {}).get('count', 0)
            analysis_sales_count = analysis.get('sales_stats', {}).get('count', 0)
            
            # Determine data quality and show warnings
            data_warnings = []
            if rental_count < 5:
                data_warnings.append(f"Limited rental data ({rental_count} flats). Analysis may not be reliable.")
            if sales_count < 5:
                data_warnings.append(f"Limited sales data ({sales_count} flats). Analysis may not be reliable.")
            if rental_count == 0 and sales_count == 0:
                data_warnings.append("No data available. Use 'Refresh Analysis' to fetch latest data.")
            
            # If analysis has errors but we have some data, still show the page
            if 'error' in analysis and (rental_count > 0 or sales_count > 0):
                flash(f"Analysis incomplete due to insufficient data. {', '.join(data_warnings)}", 'warning')
                # Create a basic analysis structure for display
                analysis = {
                    'complex_name': complex_name,
                    'query_date': query_date,
                    'area_max': area_max,
                    'rental_stats': {'count': rental_count, 'error': 'Insufficient data'},
                    'sales_stats': {'count': sales_count, 'error': 'Insufficient data'},
                    'error': 'Insufficient data for reliable analysis',
                    'insights': {
                        'price_per_sqm': {'rental': None, 'sales': None},
                        'market_position': {'rental_competitiveness': 'N/A', 'investment_potential': 'N/A'},
                        'data_quality': {
                            'rental_sample_size': rental_count,
                            'sales_sample_size': sales_count,
                            'reliability': 'Low'
                        }
                    }
                }
            elif 'error' in analysis and rental_count == 0 and sales_count == 0:
                # No data at all - show empty analysis
                flash(f"No data available for {complex_name}. Use 'Refresh Analysis' to fetch latest data.", 'info')
                analysis = {
                    'complex_name': complex_name,
                    'query_date': query_date,
                    'area_max': area_max,
                    'rental_stats': {'count': 0, 'error': 'No data available'},
                    'sales_stats': {'count': 0, 'error': 'No data available'},
                    'error': 'No data available',
                    'insights': {
                        'price_per_sqm': {'rental': None, 'sales': None},
                        'market_position': {'rental_competitiveness': 'N/A', 'investment_potential': 'N/A'},
                        'data_quality': {
                            'rental_sample_size': 0,
                            'sales_sample_size': 0,
                            'reliability': 'Low'
                        }
                    }
                }
            
            # Always render the template, even with insufficient data
            return render_template('unified_jk_view.html', 
                                analysis=analysis,
                                complex_info=complex_info,
                                complex_name=complex_name,
                                rental_flats=rental_flats,
                                sales_flats=sales_flats,
                                data_warnings=data_warnings,
                                bucket_analysis=bucket_analysis)
        finally:
            db.disconnect()
        
    except Exception as e:
        flash(f"Error analyzing complex: {str(e)}", 'error')
        return redirect(url_for('search_jk'))


@app.route('/refresh_analysis/<complex_name>', methods=['POST'])
def refresh_analysis(complex_name):
    """Refresh analysis by fetching latest data from Krisha.kz and re-analyzing."""
    try:
        # Decode the complex name from URL
        from urllib.parse import unquote
        complex_name = unquote(complex_name)
        
        print(f"🔄 Refreshing analysis for {complex_name}")
        
        # Get complex information
        complex_info = search_complex_by_name(complex_name)
        if not complex_info:
            return jsonify({'success': False, 'error': f'Complex "{complex_name}" not found'}), 404
        
        # Try to find complex ID
        complex_id = complex_info.get('complex_id') if complex_info else None
        
        # Scrape fresh data for this complex
        print(f"🔄 Scraping fresh data for {complex_name}")
        if scrape_complex_data(complex_name, complex_id):
            print(f"✅ Successfully scraped fresh data for {complex_name}")
            
            # Get analysis parameters (use same as analyze_jk)
            area_max = float(request.args.get('area_max', 1000.0))  # Match analyze_jk default
            query_date = datetime.now().strftime('%Y-%m-%d')
            
            # Re-analyze with fresh data
            analysis = analytics.get_jk_comprehensive_analysis(complex_name, area_max, query_date)
            
            if 'error' in analysis:
                return jsonify({'success': False, 'error': f'Analysis failed: {analysis["error"]}'}), 500
            
            print(f"✅ Successfully refreshed analysis for {complex_name}")
            response_data = {
                'success': True, 
                'message': f'Successfully refreshed analysis for {complex_name}',
                'rental_count': analysis.get('rental_stats', {}).get('count', 0),
                'sales_count': analysis.get('sales_stats', {}).get('count', 0)
            }
            return jsonify(response_data)
        else:
            return jsonify({'success': False, 'error': f'Failed to scrape data for {complex_name}'}), 500
            
    except Exception as e:
        print(f"❌ Error refreshing analysis for {complex_name}: {str(e)}")
        return jsonify({'success': False, 'error': f'Error refreshing analysis: {str(e)}'}), 500


@app.route('/estimate_flat', methods=['GET', 'POST'])
def estimate_flat():
    """Estimate investment potential for a specific flat."""
    # Load default area tolerance from config
    analysis_config = load_analysis_config()
    default_area_tolerance = analysis_config['default_area_tolerance']
    
    # Create investment analysis classes
    from dataclasses import dataclass
    
    @dataclass
    class DiscountScenario:
        discount: int
        discounted_price: int
        savings: int
        yield_rate: float
        price_vs_median: float
    
    @dataclass
    class InvestmentAnalysis:
        annual_rental_income: int
        rental_yield: float
        price_vs_median: float
        recommendation: str
        discount_scenarios: list
    
    if request.method == 'POST':
        flat_id = request.form.get('flat_id', '').strip()
        area_tolerance = float(request.form.get('area_tolerance', default_area_tolerance))
        
        if not flat_id:
            flash('Please enter a flat ID', 'error')
            return render_template('estimate_flat.html', default_area_tolerance=default_area_tolerance, flat_id='')
    else:
        # GET request - check for flat_id parameter
        flat_id = request.args.get('flat_id', '').strip()
        area_tolerance = float(request.args.get('area_tolerance', default_area_tolerance))
        
        # If flat_id is provided in URL, process it automatically
        if flat_id:
            print(f"🔍 Processing flat_id from URL: {flat_id}")
            # Process the flat_id automatically
            try:
                # Check if flat already exists in database first
                db = EnhancedFlatDatabase()
                db.connect()
                
                try:
                    # Check if flat exists in sales_flats table
                    cursor = db.conn.execute("""
                        SELECT flat_id, price, area, residential_complex, floor, total_floors, 
                               construction_year, parking, description, url, query_date
                        FROM sales_flats 
                        WHERE flat_id = ? 
                        ORDER BY query_date DESC 
                        LIMIT 1
                    """, (flat_id,))
                    
                    existing_flat = cursor.fetchone()
                    print(f"🔍 Database lookup for flat {flat_id}: {'Found' if existing_flat else 'Not found'}")
                    
                    if existing_flat:
                        # Use existing data from database
                        print(f"📋 Using existing flat data from database for {flat_id}")
                        flat_info = FlatInfo(
                            flat_id=existing_flat[0],
                            price=existing_flat[1],
                            area=existing_flat[2],
                            residential_complex=existing_flat[3],
                            floor=existing_flat[4],
                            total_floors=existing_flat[5],
                            construction_year=existing_flat[6],
                            parking=existing_flat[7],
                            description=existing_flat[8],
                            is_rental=False  # Sales flats are never rental
                        )
                    else:
                        # Scrape fresh data from the web
                        print(f"🌐 Scraping fresh flat data for {flat_id}")
                        flat_url = f"https://krisha.kz/a/show/{flat_id}"
                        
                        try:
                            flat_info = scrape_flat_info(flat_url)
                        except Exception as e:
                            print(f"❌ Error scraping flat {flat_id}: {e}")
                            flash(f'❌ Error: Could not scrape flat {flat_id}. Please check if the flat ID is correct.', 'error')
                            return render_template('estimate_flat.html', default_area_tolerance=default_area_tolerance, flat_id=flat_id)
                        
                        # Check if the flat is for rent
                        if flat_info.is_rental:
                            flash(f'❌ Error: Flat ID {flat_id} is for rent (Аренда). Please provide the ID of a flat for sale to get investment analysis results.', 'error')
                            return render_template('estimate_flat.html', default_area_tolerance=default_area_tolerance, flat_id=flat_id)
                        
                        # Save to database
                        query_date = datetime.now().strftime('%Y-%m-%d')
                        success = save_sales_flat_to_db(flat_info, flat_url, query_date)
                    
                    # Get similar properties and calculate investment analysis
                    area_min = flat_info.area * (1 - area_tolerance / 100)
                    area_max = flat_info.area * (1 + area_tolerance / 100)
                    
                    # Query similar rentals
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
                    
                    # Query similar sales
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
                    
                    # Calculate investment analysis
                    print(f"📊 Found {len(similar_rentals)} rental flats and {len(similar_sales)} sales flats for analysis")
                    if similar_rentals and similar_sales:
                        # Calculate rental statistics
                        rental_prices = [r[0] for r in similar_rentals]
                        avg_rental_price = sum(rental_prices) / len(rental_prices)
                        median_rental_price = sorted(rental_prices)[len(rental_prices) // 2]
                        
                        # Calculate sales statistics
                        sales_prices = [s[0] for s in similar_sales]
                        avg_sales_price = sum(sales_prices) / len(sales_prices)
                        median_sales_price = sorted(sales_prices)[len(sales_prices) // 2]
                        
                        # Calculate investment metrics
                        annual_rental_income = avg_rental_price * 12
                        rental_yield = (annual_rental_income / flat_info.price) * 100
                        price_vs_median = ((flat_info.price - median_sales_price) / median_sales_price) * 100
                        
                        # Create investment analysis object
                        
                        # Calculate discount scenarios
                        discount_scenarios = []
                        for discount in [5, 10, 15, 20]:
                            discounted_price = flat_info.price * (1 - discount / 100)
                            scenario_yield = (annual_rental_income / discounted_price) * 100
                            scenario_price_vs_median = ((discounted_price - median_sales_price) / median_sales_price) * 100
                            
                            discount_scenarios.append(DiscountScenario(
                                discount=discount,
                                discounted_price=int(discounted_price),
                                savings=int(flat_info.price - discounted_price),
                                yield_rate=scenario_yield,
                                price_vs_median=scenario_price_vs_median
                            ))
                        
                        # Determine recommendation
                        if rental_yield > 20:
                            recommendation = "🚀 STRONG BUY"
                        elif rental_yield > 8 and price_vs_median < 0:
                            recommendation = "✅ BUY"
                        elif rental_yield > 5:
                            recommendation = "⚖️ CONSIDER"
                        else:
                            recommendation = "❌ PASS"
                        
                        investment_analysis = InvestmentAnalysis(
                            annual_rental_income=int(annual_rental_income),
                            rental_yield=rental_yield,
                            price_vs_median=price_vs_median,
                            recommendation=recommendation,
                            discount_scenarios=discount_scenarios
                        )
                        
                        # Calculate rental statistics for template
                        rental_prices = [r[0] for r in similar_rentals]
                        rental_stats = {
                            'count': len(similar_rentals),
                            'min_price': min(rental_prices),
                            'max_price': max(rental_prices),
                            'avg_price': sum(rental_prices) / len(rental_prices),
                            'median_price': sorted(rental_prices)[len(rental_prices) // 2]
                        }
                        
                        # Calculate sales statistics for template
                        sales_prices = [s[0] for s in similar_sales]
                        sales_stats = {
                            'count': len(similar_sales),
                            'min_price': min(sales_prices),
                            'max_price': max(sales_prices),
                            'avg_price': sum(sales_prices) / len(sales_prices),
                            'median_price': sorted(sales_prices)[len(sales_prices) // 2]
                        }
                        
                        print(f"📊 Rental stats: {rental_stats}")
                        print(f"📊 Sales stats: {sales_stats}")
                        print(f"📊 Investment analysis: {investment_analysis}")
                        
                        # Render results template
                        print(f"🔍 Rendering template with:")
                        print(f"  - flat_info: {flat_info}")
                        print(f"  - investment_analysis: {investment_analysis}")
                        print(f"  - rental_stats: {rental_stats}")
                        print(f"  - sales_stats: {sales_stats}")
                        print(f"  - area_tolerance: {area_tolerance}")
                        
                        # Convert dataclass to dictionary for template compatibility
                        investment_analysis_dict = {
                            'annual_rental_income': investment_analysis.annual_rental_income,
                            'rental_yield': investment_analysis.rental_yield,
                            'price_vs_median': investment_analysis.price_vs_median,
                            'recommendation': investment_analysis.recommendation,
                            'discount_scenarios': [
                                {
                                    'discount': scenario.discount,
                                    'discounted_price': scenario.discounted_price,
                                    'savings': scenario.savings,
                                    'yield_rate': scenario.yield_rate,
                                    'price_vs_median': scenario.price_vs_median
                                }
                                for scenario in investment_analysis.discount_scenarios
                            ]
                        }
                        
                        # Convert FlatInfo to dictionary as well
                        flat_info_dict = {
                            'flat_id': flat_info.flat_id,
                            'price': flat_info.price,
                            'area': flat_info.area,
                            'residential_complex': flat_info.residential_complex,
                            'floor': flat_info.floor,
                            'total_floors': flat_info.total_floors,
                            'construction_year': flat_info.construction_year,
                            'parking': flat_info.parking,
                            'description': flat_info.description,
                            'is_rental': flat_info.is_rental
                        }
                        
                        # Pre-format stats for robust template rendering
                        rental_stats_fmt = {
                            'count': rental_stats['count'],
                            'min_price': f"{int(rental_stats['min_price']):,}",
                            'max_price': f"{int(rental_stats['max_price']):,}",
                            'avg_price': f"{rental_stats['avg_price']:.0f}",
                            'median_price': f"{int(rental_stats['median_price']):,}"
                        }
                        sales_stats_fmt = {
                            'count': sales_stats['count'],
                            'min_price': f"{int(sales_stats['min_price']):,}",
                            'max_price': f"{int(sales_stats['max_price']):,}",
                            'avg_price': f"{sales_stats['avg_price']:.0f}",
                            'median_price': f"{int(sales_stats['median_price']):,}"
                        }
                        
                        print(f"🔍 flat_info_dict created: {flat_info_dict}")
                        
                        try:
                            return render_template('estimate_result.html', 
                                                flat_info=flat_info_dict,
                                                investment_analysis=investment_analysis_dict,
                                                rental_stats=rental_stats,
                                                sales_stats=sales_stats,
                                                rental_stats_fmt=rental_stats_fmt,
                                                sales_stats_fmt=sales_stats_fmt,
                                                area_tolerance=area_tolerance)
                        except Exception as e:
                            print(f"❌ Template rendering error: {e}")
                            import traceback
                            traceback.print_exc()
                            flash(f'❌ Error rendering results: {str(e)}', 'error')
                            return render_template('estimate_flat.html', default_area_tolerance=default_area_tolerance, flat_id=flat_id)
                    else:
                        print(f"⚠️ Insufficient data: {len(similar_rentals)} rental flats, {len(similar_sales)} sales flats")
                        
                        # Try to automatically scrape complex data if we have a residential complex
                        if flat_info.residential_complex and (len(similar_rentals) < 3 or len(similar_sales) < 3):
                            print(f"🔄 Attempting to scrape complex data for {flat_info.residential_complex}")
                            flash(f'⚠️ Insufficient data for analysis. Found {len(similar_rentals)} rental and {len(similar_sales)} sales flats. Automatically fetching latest data from Krisha.kz...', 'warning')
                            
                            # Try to find complex ID
                            complex_info = search_complex_by_name(flat_info.residential_complex)
                            complex_id = complex_info.get('complex_id') if complex_info else None
                            
                            # Scrape data for this complex
                            if scrape_complex_data(flat_info.residential_complex, complex_id):
                                flash(f"✅ Successfully scraped data for {flat_info.residential_complex}. Re-analyzing...", 'success')
                                
                                # Re-query similar properties after scraping
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
                                
                                print(f"📊 After scraping: {len(similar_rentals)} rental flats, {len(similar_sales)} sales flats")
                                
                                # Try analysis again
                                if similar_rentals and similar_sales:
                                    # Calculate rental statistics
                                    rental_prices = [r[0] for r in similar_rentals]
                                    avg_rental_price = sum(rental_prices) / len(rental_prices)
                                    median_rental_price = sorted(rental_prices)[len(rental_prices) // 2]
                                    
                                    # Calculate sales statistics
                                    sales_prices = [s[0] for s in similar_sales]
                                    avg_sales_price = sum(sales_prices) / len(sales_prices)
                                    median_sales_price = sorted(sales_prices)[len(sales_prices) // 2]
                                    
                                    # Calculate investment metrics
                                    annual_rental_income = avg_rental_price * 12
                                    rental_yield = (annual_rental_income / flat_info.price) * 100
                                    price_vs_median = ((flat_info.price - median_sales_price) / median_sales_price) * 100
                                    
                                    # Calculate discount scenarios
                                    discount_scenarios = []
                                    for discount in [5, 10, 15, 20]:
                                        discounted_price = flat_info.price * (1 - discount / 100)
                                        scenario_yield = (annual_rental_income / discounted_price) * 100
                                        scenario_price_vs_median = ((discounted_price - median_sales_price) / median_sales_price) * 100
                                        
                                        discount_scenarios.append(DiscountScenario(
                                            discount=discount,
                                            discounted_price=int(discounted_price),
                                            savings=int(flat_info.price - discounted_price),
                                            yield_rate=scenario_yield,
                                            price_vs_median=scenario_price_vs_median
                                        ))
                                    
                                    # Determine recommendation
                                    if rental_yield > 20:
                                        recommendation = "🚀 STRONG BUY"
                                    elif rental_yield > 8 and price_vs_median < 0:
                                        recommendation = "✅ BUY"
                                    elif rental_yield > 5:
                                        recommendation = "⚖️ CONSIDER"
                                    else:
                                        recommendation = "❌ PASS"
                                    
                                    investment_analysis = InvestmentAnalysis(
                                        annual_rental_income=int(annual_rental_income),
                                        rental_yield=rental_yield,
                                        price_vs_median=price_vs_median,
                                        recommendation=recommendation,
                                        discount_scenarios=discount_scenarios
                                    )
                                    
                                    # Calculate rental statistics for template
                                    rental_prices = [r[0] for r in similar_rentals]
                                    rental_stats = {
                                        'count': len(similar_rentals),
                                        'min_price': min(rental_prices),
                                        'max_price': max(rental_prices),
                                        'avg_price': sum(rental_prices) / len(rental_prices),
                                        'median_price': sorted(rental_prices)[len(rental_prices) // 2]
                                    }
                                    
                                    # Calculate sales statistics for template
                                    sales_prices = [s[0] for s in similar_sales]
                                    sales_stats = {
                                        'count': len(similar_sales),
                                        'min_price': min(sales_prices),
                                        'max_price': max(sales_prices),
                                        'avg_price': sum(sales_prices) / len(sales_prices),
                                        'median_price': sorted(sales_prices)[len(sales_prices) // 2]
                                    }
                                    
                                    print(f"📊 Rental stats: {rental_stats}")
                                    print(f"📊 Sales stats: {sales_stats}")
                                    print(f"📊 Investment analysis: {investment_analysis}")
                                    
                                    # Convert FlatInfo to dictionary for template compatibility
                                    flat_info_dict = {
                                        'flat_id': flat_info.flat_id,
                                        'price': flat_info.price,
                                        'area': flat_info.area,
                                        'residential_complex': flat_info.residential_complex,
                                        'floor': flat_info.floor,
                                        'total_floors': flat_info.total_floors,
                                        'construction_year': flat_info.construction_year,
                                        'parking': flat_info.parking,
                                        'description': flat_info.description,
                                        'is_rental': flat_info.is_rental
                                    }
                                    
                                    # Convert investment_analysis to dictionary
                                    investment_analysis_dict = {
                                        'annual_rental_income': investment_analysis.annual_rental_income,
                                        'rental_yield': investment_analysis.rental_yield,
                                        'price_vs_median': investment_analysis.price_vs_median,
                                        'recommendation': investment_analysis.recommendation,
                                        'discount_scenarios': [
                                            {
                                                'discount': scenario.discount,
                                                'discounted_price': scenario.discounted_price,
                                                'savings': scenario.savings,
                                                'yield_rate': scenario.yield_rate,
                                                'price_vs_median': scenario.price_vs_median
                                            }
                                            for scenario in investment_analysis.discount_scenarios
                                        ]
                                    }
                                    
                                    return render_template('estimate_result.html', 
                                                        flat_info=flat_info_dict,
                                                        investment_analysis=investment_analysis_dict,
                                                        rental_stats=rental_stats,
                                                        sales_stats=sales_stats,
                                                        area_tolerance=area_tolerance)
                                else:
                                    flash(f'❌ Still insufficient data after scraping. Found {len(similar_rentals)} rental and {len(similar_sales)} sales flats.', 'error')
                                    return render_template('estimate_flat.html', default_area_tolerance=default_area_tolerance, flat_id=flat_id)
                            else:
                                flash(f'❌ Failed to scrape data for {flat_info.residential_complex}. Insufficient data for analysis.', 'error')
                                return render_template('estimate_flat.html', default_area_tolerance=default_area_tolerance, flat_id=flat_id)
                        else:
                            flash(f'❌ Insufficient data for analysis. Found {len(similar_rentals)} rental and {len(similar_sales)} sales flats.', 'error')
                            return render_template('estimate_flat.html', default_area_tolerance=default_area_tolerance, flat_id=flat_id)
                        
                finally:
                    db.disconnect()
                    
            except Exception as e:
                print(f"❌ Error analyzing flat {flat_id}: {str(e)}")
                flash(f'❌ Error analyzing flat {flat_id}: {str(e)}', 'error')
                return render_template('estimate_flat.html', default_area_tolerance=default_area_tolerance, flat_id=flat_id)
        
        # No flat_id provided, show the form
        return render_template('estimate_flat.html', default_area_tolerance=default_area_tolerance, flat_id='')
    
    # POST method processing continues here
    try:
        # Check if flat already exists in database first
        db = EnhancedFlatDatabase()
        db.connect()
        
        try:
            # Check if flat exists in sales_flats table
            cursor = db.conn.execute("""
                SELECT flat_id, price, area, residential_complex, floor, total_floors, 
                       construction_year, parking, description, url
                FROM sales_flats 
                WHERE flat_id = ? 
                ORDER BY query_date DESC 
                LIMIT 1
            """, (flat_id,))
            
            existing_flat = cursor.fetchone()
            
            if existing_flat:
                # Use existing data from database
                print(f"📋 Using existing flat data from database for {flat_id}")
                flat_info = FlatInfo(
                    flat_id=existing_flat[0],
                    price=existing_flat[1],
                    area=existing_flat[2],
                    residential_complex=existing_flat[3],
                    floor=existing_flat[4],
                    total_floors=existing_flat[5],
                    construction_year=existing_flat[6],
                    parking=existing_flat[7],
                    description=existing_flat[8],
                    is_rental=False  # Sales flats are never rental
                )
            else:
                # Scrape fresh data from the web
                print(f"🌐 Scraping fresh flat data for {flat_id}")
                flat_url = f"https://krisha.kz/a/show/{flat_id}"
                flat_info = scrape_flat_info(flat_url)
                
                # Check if the flat is for rent
                if flat_info.is_rental:
                    flash(f'❌ Error: Flat ID {flat_id} is for rent (Аренда). Please provide the ID of a flat for sale to get investment analysis results.', 'error')
                    return render_template('estimate_flat.html', default_area_tolerance=default_area_tolerance, flat_id=flat_id)
                
                # Save to database
                query_date = datetime.now().strftime('%Y-%m-%d')
                success = save_sales_flat_to_db(flat_info, flat_url, query_date)
            
            # Get similar properties
            # Calculate area range
            area_min = flat_info.area * (1 - area_tolerance / 100)
            area_max = flat_info.area * (1 + area_tolerance / 100)
            
            # Query similar rentals
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
                    rental_data[flat_id] = row[1:]
            
            similar_rentals = list(rental_data.values())
            
            # Query similar sales
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
                    sales_data[flat_id] = row[1:]
            
            similar_sales = list(sales_data.values())
            
            # Calculate investment analysis
            if similar_rentals and similar_sales:
                # Calculate rental statistics
                rental_prices = [r[0] for r in similar_rentals]
                avg_rental_price = sum(rental_prices) / len(rental_prices)
                median_rental_price = sorted(rental_prices)[len(rental_prices) // 2]
                
                # Calculate sales statistics
                sales_prices = [s[0] for s in similar_sales]
                avg_sales_price = sum(sales_prices) / len(sales_prices)
                median_sales_price = sorted(sales_prices)[len(sales_prices) // 2]
                
                # Calculate investment metrics
                annual_rental_income = avg_rental_price * 12
                rental_yield = (annual_rental_income / flat_info.price) * 100
                price_vs_median = ((flat_info.price - median_sales_price) / median_sales_price) * 100
                
                # Create investment analysis object
                
                # Calculate discount scenarios
                discount_scenarios = []
                for discount in [5, 10, 15, 20]:
                    discounted_price = flat_info.price * (1 - discount / 100)
                    scenario_yield = (annual_rental_income / discounted_price) * 100
                    scenario_price_vs_median = ((discounted_price - median_sales_price) / median_sales_price) * 100
                    
                    discount_scenarios.append(DiscountScenario(
                        discount=discount,
                        discounted_price=int(discounted_price),
                        savings=int(flat_info.price - discounted_price),
                        yield_rate=scenario_yield,
                        price_vs_median=scenario_price_vs_median
                    ))
                
                # Determine recommendation
                if rental_yield > 20:
                    recommendation = "🚀 STRONG BUY"
                elif rental_yield > 8 and price_vs_median < 0:
                    recommendation = "✅ BUY"
                elif rental_yield > 5:
                    recommendation = "⚖️ CONSIDER"
                else:
                    recommendation = "❌ PASS"
                
                investment_analysis = InvestmentAnalysis(
                    annual_rental_income=int(annual_rental_income),
                    rental_yield=rental_yield,
                    price_vs_median=price_vs_median,
                    recommendation=recommendation,
                    discount_scenarios=discount_scenarios
                )
                
                # Render results template
                return render_template('estimate_result.html', 
                                    flat_info=flat_info,
                                    investment_analysis=investment_analysis,
                                    rental_stats={'count': len(similar_rentals)},
                                    sales_stats={'count': len(similar_sales)})
            else:
                flash(f'❌ Insufficient data for analysis. Found {len(similar_rentals)} rental and {len(similar_sales)} sales flats.', 'error')
                return render_template('estimate_flat.html', default_area_tolerance=default_area_tolerance, flat_id=flat_id)
                
        finally:
            db.disconnect()
            
    except Exception as e:
        flash(f'❌ Error analyzing flat {flat_id}: {str(e)}', 'error')
        return render_template('estimate_flat.html', default_area_tolerance=default_area_tolerance, flat_id=flat_id)


@app.route('/flats/<complex_name>')
def view_jk_flats(complex_name):
    """Display all flats for a specific residential complex."""
    # Redirect to the unified view
    return redirect(url_for('analyze_jk', complex_name=complex_name))


@app.route('/similar_flats/<flat_type>/<complex_name>')
def view_similar_flats(flat_type, complex_name):
    """Display similar flats for a specific complex and type."""
    try:
        # Get complex information
        complex_info = search_complex_by_name(complex_name)
        if not complex_info:
            flash(f'Complex "{complex_name}" not found', 'error')
            return redirect(url_for('search_jk'))
        
        # Get similar flats for this complex
        db = EnhancedFlatDatabase()
        try:
            # Get flats by type (rental or sales)
            flats = db.get_flats_by_complex(complex_name, flat_type)
            
            # If no flats found, try to scrape data
            if not flats:
                flash(f"No {flat_type} flats found for {complex_name}. Attempting to scrape data...", 'info')
                
                # Try to find complex ID
                complex_id = complex_info.get('complex_id') if complex_info else None
                
                # Scrape data for this complex
                if scrape_complex_data(complex_name, complex_id):
                    flash(f"Successfully scraped data for {complex_name}. Reloading...", 'success')
                    
                    # Get flats again after scraping
                    flats = db.get_flats_by_complex(complex_name, flat_type)
                else:
                    flash(f"Failed to scrape data for {complex_name}", 'warning')
            
            return render_template('view_similar_flats.html', 
                                complex_info=complex_info,
                                flats=flats,
                                flat_type=flat_type)
        finally:
            db.disconnect()
            
    except Exception as e:
        flash(f'Error loading similar flats for {complex_name}: {str(e)}', 'error')
        return redirect(url_for('search_jk'))


@app.route('/favorites')
def favorites():
    """Display user's favorite flats."""
    try:
        db = EnhancedFlatDatabase()
        favorites_list = db.get_favorites()
        db.disconnect()
        
        return render_template('favorites.html', favorites=favorites_list)
        
    except Exception as e:
        flash(f'Error loading favorites: {str(e)}', 'error')
        return redirect(url_for('index'))


@app.route('/api/favorites/add', methods=['POST'])
def add_to_favorites():
    """API endpoint to add a flat to favorites."""
    try:
        data = request.get_json()
        print(f"🔍 API /api/favorites/add received data: {data}")
        
        flat_id = data.get('flat_id')
        flat_type = data.get('flat_type')  # 'rental' or 'sale'
        notes = data.get('notes', '')
        flat_data = data.get('flat_data')  # Direct flat data from frontend
        
        print(f"🔍 Parsed values: flat_id={flat_id}, flat_type={flat_type}, flat_data={flat_data}")
        
        if not flat_id or not flat_type:
            print(f"❌ Error: Missing flat_id or flat_type. flat_id='{flat_id}', flat_type='{flat_type}'")
            return jsonify({'success': False, 'error': 'Missing flat_id or flat_type'}), 400
        
        db = EnhancedFlatDatabase()
        try:
            # If flat_data is provided directly from frontend, use it
            if flat_data:
                flat_dict = {
                    'flat_id': flat_id,
                    'price': flat_data.get('price', 0),
                    'area': flat_data.get('area', 0),
                    'residential_complex': flat_data.get('residential_complex', ''),
                    'floor': flat_data.get('floor'),
                    'total_floors': flat_data.get('total_floors'),
                    'construction_year': flat_data.get('construction_year'),
                    'parking': flat_data.get('parking', ''),
                    'description': flat_data.get('description', ''),
                    'url': flat_data.get('url', f'https://krisha.kz/a/show/{flat_id}')
                }
            else:
                # Try to get flat data from database
                if flat_type == 'rental':
                    cursor = db.conn.execute("""
                        SELECT flat_id, price, area, residential_complex, floor, total_floors,
                               construction_year, parking, description, url
                        FROM rental_flats 
                        WHERE flat_id = ? 
                        ORDER BY query_date DESC 
                        LIMIT 1
                    """, (flat_id,))
                else:
                    cursor = db.conn.execute("""
                        SELECT flat_id, price, area, residential_complex, floor, total_floors,
                               construction_year, parking, description, url
                        FROM sales_flats 
                        WHERE flat_id = ? 
                        ORDER BY query_date DESC 
                        LIMIT 1
                    """, (flat_id,))
                
                flat_data = cursor.fetchone()
                if not flat_data:
                    return jsonify({'success': False, 'error': 'Flat not found in database'}), 404
                
                # Convert to dict
                flat_dict = dict(flat_data)
            
            # Add to favorites
            success = db.add_to_favorites(flat_dict, flat_type, notes)
            
            if success:
                return jsonify({'success': True, 'message': 'Added to favorites'})
            else:
                return jsonify({'success': False, 'error': 'Failed to add to favorites'}), 500
                
        finally:
            db.disconnect()
            
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/favorites/remove', methods=['POST'])
def remove_from_favorites():
    """API endpoint to remove a flat from favorites."""
    try:
        data = request.get_json()
        flat_id = data.get('flat_id')
        flat_type = data.get('flat_type')
        
        if not flat_id or not flat_type:
            return jsonify({'success': False, 'error': 'Missing flat_id or flat_type'}), 400
        
        db = EnhancedFlatDatabase()
        try:
            success = db.remove_from_favorites(flat_id, flat_type)
            
            if success:
                return jsonify({'success': True, 'message': 'Removed from favorites'})
            else:
                return jsonify({'success': False, 'error': 'Failed to remove from favorites'}), 500
                
        finally:
            db.disconnect()
            
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/favorites/check', methods=['POST'])
def check_favorite_status():
    """API endpoint to check if a flat is in favorites."""
    try:
        data = request.get_json()
        print(f"🔍 API /api/favorites/check received data: {data}")
        
        flat_id = data.get('flat_id')
        flat_type = data.get('flat_type')
        
        print(f"🔍 Parsed values: flat_id={flat_id}, flat_type={flat_type}")
        
        if not flat_id or not flat_type:
            print(f"❌ Error: Missing flat_id or flat_type. flat_id='{flat_id}', flat_type='{flat_type}'")
            return jsonify({'success': False, 'error': 'Missing flat_id or flat_type'}), 400
        
        db = EnhancedFlatDatabase()
        try:
            is_favorite = db.is_favorite(flat_id, flat_type)
            return jsonify({'success': True, 'is_favorite': is_favorite})
        finally:
            db.disconnect()
            
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/fix_flat_classification/<flat_id>/<correct_type>')
def fix_flat_classification(flat_id: str, correct_type: str):
    """Fix flat classification by moving it to the correct table."""
    try:
        db = EnhancedFlatDatabase()
        success = db.move_flat_to_correct_table(flat_id, correct_type)
        db.disconnect()
        
        if success:
            flash(f'Successfully moved flat {flat_id} to {correct_type} table', 'success')
        else:
            flash(f'Failed to move flat {flat_id}', 'error')
            
        return redirect(url_for('search_jk'))
        
    except Exception as e:
        flash(f'Error fixing flat classification: {str(e)}', 'error')
        return redirect(url_for('search_jk'))


@app.route('/api/complexes')
def api_complexes():
    """API endpoint to get all complexes for autocomplete."""
    complexes = get_all_residential_complexes()
    return jsonify([{'id': c['complex_id'], 'name': c['name']} for c in complexes])


@app.route('/api/update_exchange_rates', methods=['POST'])
def update_exchange_rates():
    """API endpoint to update exchange rates from mig.kz."""
    try:
        success = currency_manager.update_exchange_rates()
        if success:
            return jsonify({'success': True, 'message': 'Exchange rates updated successfully'})
        else:
            return jsonify({'success': False, 'error': 'Failed to update exchange rates'}), 500
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/exchange_rates')
def get_exchange_rates():
    """API endpoint to get current exchange rates."""
    try:
        eur_rate = currency_manager.get_latest_rate('EUR')
        usd_rate = currency_manager.get_latest_rate('USD')
        
        return jsonify({
            'EUR': eur_rate,
            'USD': usd_rate,
            'last_updated': datetime.now().isoformat()
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000) 