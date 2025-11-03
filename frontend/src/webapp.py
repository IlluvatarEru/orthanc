#!/usr/bin/env python3
"""
Clean, simple Flask webapp that fails fast and never silently catches errors.
"""

import logging
from datetime import datetime
from urllib.parse import unquote

from flask import Flask, render_template, request, jsonify, redirect, url_for

from api.src.analysis_objects import SalesAnalysisResponse, FlatTypeStats, Opportunity
from common.src.flat_info import FlatInfo
# Import our modules
from db.src.write_read_database import OrthancDB
from frontend.src.webapp_api_client import WebappAPIClient

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize Flask app
app = Flask(__name__, template_folder='../templates', static_folder='../static')
app.secret_key = 'your-secret-key-here'


# Context processor to make currency preference available in all templates
@app.context_processor
def inject_currency_preference():
    show_eur = request.cookies.get('showEur', 'false') == 'true'

    # Get EUR exchange rate
    db = OrthancDB()
    eur_rate = db.get_latest_rate('EUR')
    db.disconnect()
    if eur_rate is None:
        raise Exception(f"eur_rate is None")
    return dict(show_eur=show_eur, eur_rate=eur_rate)


# Initialize API client
api_client = WebappAPIClient()


@app.route('/')
def index():
    """Dashboard home page."""
    stats_result = api_client.get_database_stats()

    if not stats_result["success"]:
        raise Exception(f"Database stats API failed: {stats_result['error']}")

    stats = stats_result["stats"]
    return render_template('index.html',
                           total_flats=stats['total_flats'],
                           rental_flats=stats['rental_flats'],
                           sales_flats=stats['sales_flats'],
                           complexes=stats['complexes'],
                           new_flats=stats['new_flats'],
                           new_rentals=stats['new_rentals'],
                           new_sales=stats['new_sales'],
                           new_complexes=stats['new_complexes'])


@app.route('/search_jk', methods=['GET', 'POST'])
def search_jk():
    """Search for residential complexes."""
    if request.method == 'POST':
        search_term = request.form.get('complex_name', '').strip()
        if not search_term:
            raise Exception("Search term is required")

        result = api_client.search_complexes(search_term)

        if not result["success"]:
            raise Exception(f"Search API failed: {result['error']}")

        complexes = result["complexes"]
        deduplication_info = result["deduplication_info"]

        return render_template('search_jk.html',
                               complexes=complexes,
                               search_term=search_term,
                               deduplication_info=deduplication_info)

    return render_template('search_jk.html', complexes=[], search_term='')


@app.route('/analyze_jk/<residential_complex_name>')
def analyze_jk(residential_complex_name, db_path='flats.db', allow_scraping_data_if_not_in_db=False):
    """Analyze a specific residential complex."""
    # Get analysis parameters
    area_max = float(request.args.get('area_max', 1000.0))
    query_date = request.args.get('date', datetime.now().strftime('%Y-%m-%d'))

    # Get complex information using API
    complex_info = api_client.get_complex_info(residential_complex_name)
    if not complex_info:
        raise Exception(f"Complex info API failed for: {residential_complex_name}")

    # Get all flats for this complex
    db = OrthancDB(db_path)
    rental_flats: list[FlatInfo] = db.get_flats_for_residential_complex(residential_complex_name, 'rental')
    sales_flats: list[FlatInfo] = db.get_flats_for_residential_complex(residential_complex_name, 'sales')
    db.disconnect()

    # If no flats found, scrape data
    if not rental_flats and not sales_flats and allow_scraping_data_if_not_in_db:
        logger.info(f"No data in db, scraping it")
        complex_id = complex_info['complex_id']
        scrape_result = api_client.scrape_complex_data(residential_complex_name, complex_id)

        if not scrape_result["success"]:
            raise Exception(f"Failed to scrape data for {residential_complex_name}: {scrape_result['error']}")

        # Get flats again after scraping
        db = OrthancDB(db_path)
        rental_flats = db.get_flats_for_residential_complex(residential_complex_name, 'rental')
        sales_flats = db.get_flats_for_residential_complex(residential_complex_name, 'sales')
        db.disconnect()

    # Get analysis using API
    sales_analysis: SalesAnalysisResponse = api_client.get_jk_sales_analysis(residential_complex_name, 0.20)
    flat_type_buckets_stats: dict[str, FlatTypeStats] = sales_analysis.current_market.flat_type_buckets
    opportunities_by_flat_type: dict[str, list[Opportunity]] = sales_analysis.current_market.opportunities
    if not sales_analysis.success:
        raise Exception(f"Sales analysis API failed: {sales_analysis.error}")

    rental_analysis = api_client.get_jk_rentals_analysis(residential_complex_name, 0.05)
    if not rental_analysis.success:
        raise Exception(f"Rental analysis API failed: {rental_analysis.error}")

    # Create analysis object for template compatibility - fail fast on missing data
    sales_global = sales_analysis.current_market.global_stats
    rental_global = rental_analysis.current_market.global_stats

    analysis = {
        'query_date': query_date,
        'area_max': area_max,
        'residential_complex_name': residential_complex_name,
        'rental_stats': {
            'count': rental_global.count,
            'price_stats': {
                'min': 0,  # Rental API doesn't provide price range, only yield stats
                'max': 0,  # Rental API doesn't provide price range, only yield stats
                'avg': 0,  # Rental API doesn't provide price stats, only yield stats
                'median': 0  # Rental API doesn't provide price stats, only yield stats
            },
            'area_stats': {
                'avg': 0  # Rental API doesn't provide area stats
            }
        },
        'sales_stats': {
            'count': sales_global.count,
            'price_stats': {
                'min': sales_global.min,
                'max': sales_global.max,
                'avg': sales_global.mean,
                'median': sales_global.median
            },
            'area_stats': {
                'avg': 0  # Sales API doesn't provide area stats
            }
        },
        'insights': {
            'price_per_sqm': {
                'rental': 0,  # Rental API doesn't provide price_per_sqm
                'sales': 0  # Sales API doesn't provide price_per_sqm
            },
            'market_position': {
                'investment_potential': 'Medium'  # Default value
            },
            'data_quality': {
                'reliability': 'High' if (rental_global.count > 5 and sales_global.count > 5) else 'Medium',
                'rental_sample_size': rental_global.count,
                'sales_sample_size': sales_global.count
            }
        },
        'error': None  # No error by default
    }

    # Extract median values for template compatibility - fail fast on missing data
    rental_median = rental_global.median_yield  # Rental API provides yield stats
    sales_median = sales_global.median  # Sales API provides price stats

    # Extract flat type buckets and opportunities from sales analysis
    flat_type_buckets = sales_analysis.current_market.flat_type_buckets
    opportunities_by_flat_type = sales_analysis.current_market.opportunities

    # Debug: Check what we're getting
    print(f"DEBUG: flat_type_buckets: {flat_type_buckets}")
    print(f"DEBUG: opportunities_by_flat_type: {opportunities_by_flat_type}")

    return render_template('unified_jk_view.html',
                           complex_info=complex_info,
                           residential_complex_name=residential_complex_name,
                           rental_flats=rental_flats,
                           sales_flats=sales_flats,
                           sales_analysis=sales_analysis,
                           rental_analysis=rental_analysis,
                           query_date=query_date,
                           area_max=area_max,
                           analysis=analysis,
                           rental_median=rental_median,
                           sales_median=sales_median,
                           flat_type_buckets=flat_type_buckets,
                           opportunities_by_flat_type=opportunities_by_flat_type)


@app.route('/flat/<flat_id>')
def view_flat_details(flat_id, area_tolerance=10.0):
    """View detailed flat information with bucket comparison - unified route for both flat ID search and opportunities."""
    # Get flat information using API
    flat_data = api_client.get_flat_info(flat_id)
    if not flat_data:
        raise Exception(f"Flat {flat_id} not found")

    # Get similar flats using API
    similar_result = api_client.get_similar_flats(flat_id, area_tolerance, 3)
    if not similar_result["success"]:
        raise Exception(f"Failed to get similar flats: {similar_result['error']}")

    similar_rentals = similar_result["similar_rentals"]
    similar_sales = similar_result["similar_sales"]

    if not similar_rentals or not similar_sales:
        raise Exception(
            f"Insufficient data for analysis. Found {len(similar_rentals)} rental and {len(similar_sales)} sales flats.")

    # Calculate investment analysis
    investment_analysis = calculate_investment_analysis(flat_data, similar_rentals, similar_sales, area_tolerance)

    # Calculate median price properly
    if similar_sales:
        sorted_prices = sorted([flat['price'] for flat in similar_sales])
        n = len(sorted_prices)
        if n % 2 == 0:
            median_price = (sorted_prices[n // 2 - 1] + sorted_prices[n // 2]) / 2
        else:
            median_price = sorted_prices[n // 2]
        mean_price = sum(flat['price'] for flat in similar_sales) / len(similar_sales)
        min_price = min(flat['price'] for flat in similar_sales)
        max_price = max(flat['price'] for flat in similar_sales)
    else:
        median_price = 0
        mean_price = 0
        min_price = 0
        max_price = 0

    # Calculate buy and sell return percentage
    current_price = flat_data['price']
    buy_sell_return_percentage = 0
    buy_sell_net_return_percentage = 0
    equivalent_annual_net_return = 0

    if median_price > 0 and current_price > 0:
        # Expected return (gross)
        buy_sell_return_percentage = ((median_price - current_price) / current_price) * 100

        # Expected net return (after 10% tax on profit and 500k notary fee)
        tax_rate = 0.10  # 10% tax on profit only
        notary_fee = 500000  # 500k notary fee in KZT
        profit = median_price - current_price  # Profit = sale price - buy price
        sale_tax = profit * tax_rate  # Tax is 10% of profit, not sale price
        net_sale_proceeds = median_price - sale_tax - notary_fee
        net_profit = net_sale_proceeds - current_price
        buy_sell_net_return_percentage = (net_profit / current_price) * 100

        # Equivalent annual net return (LINEAR - assuming 2M holding period)
        holding_period_years = 2 / 12
        equivalent_annual_net_return = buy_sell_net_return_percentage / holding_period_years

    # Create opportunity-like object for template compatibility
    opportunity = type('Opportunity', (), {
        'flat_id': flat_id,
        'price': flat_data['price'],
        'area': flat_data['area'],
        'flat_type': flat_data['flat_type'],
        'residential_complex': flat_data['residential_complex'],
        'floor': flat_data['floor'],
        'total_floors': flat_data['total_floors'],
        'construction_year': flat_data['construction_year'],
        'parking': flat_data['parking'],
        'description': flat_data['description'],
        'discount_percentage_vs_median': None,  # Will be calculated
        'market_stats': {
            'median_price': median_price,
            'mean_price': mean_price,
            'min_price': min_price,
            'max_price': max_price,
            'count': len(similar_sales)
        },
        'bucket_flats': similar_sales  # Use similar sales as bucket flats for comparison
    })()

    return render_template('unified_flat_view.html',
                           flat_data=flat_data,
                           opportunity=opportunity,
                           similar_rentals=similar_rentals,
                           similar_sales=similar_sales,
                           investment_analysis=investment_analysis,
                           buy_sell_return_percentage=buy_sell_return_percentage,
                           buy_sell_net_return_percentage=buy_sell_net_return_percentage,
                           equivalent_annual_net_return=equivalent_annual_net_return,
                           area_tolerance=area_tolerance)


@app.route('/refresh_analysis/<residential_complex_name>', methods=['POST'])
def refresh_analysis(residential_complex_name):
    """Refresh analysis by fetching latest data."""
    residential_complex_name = unquote(residential_complex_name)

    # Get complex information
    complex_info = api_client.get_complex_info(residential_complex_name)
    if not complex_info:
        raise Exception(f"Complex info API failed for refresh: {residential_complex_name}")

    # Refresh analysis using API
    refresh_result = api_client.refresh_complex_analysis(residential_complex_name)
    if not refresh_result["success"]:
        raise Exception(f"Refresh analysis API failed: {refresh_result['error']}")

    return jsonify({
        'success': True,
        'message': f'Successfully refreshed analysis for {residential_complex_name}'
    })


def extract_flat_id_from_input(user_input):
    """
    Extract flat ID from either a Krisha URL or a plain ID.
    
    :param user_input: str, either a Krisha URL or flat ID
    :return: str, extracted flat ID
    """
    user_input = user_input.strip()

    # If it's already just a number/ID, return it
    if user_input.isdigit():
        return user_input

    # If it's a Krisha URL, extract the ID
    if 'krisha.kz/a/show/' in user_input:
        # Extract ID from URL like https://krisha.kz/a/show/1000383479
        import re
        match = re.search(r'/a/show/(\d+)', user_input)
        if match:
            return match.group(1)

    # If it's some other format, try to extract any number
    import re
    numbers = re.findall(r'\d+', user_input)
    if numbers:
        return numbers[-1]  # Return the last number found

    # If no number found, return the original input
    return user_input


@app.route('/estimate_flat', methods=['GET', 'POST'])
def estimate_flat():
    """Estimate flat investment potential."""
    if request.method == 'POST':
        user_input = request.form.get('flat_id', '').strip()
        area_tolerance = float(request.form.get('area_tolerance', 10.0))

        if not user_input:
            raise Exception("Flat ID or Krisha URL is required")

        # Extract flat ID from user input (handles both URLs and plain IDs)
        flat_id = extract_flat_id_from_input(user_input)

        # Redirect to the unified flat view page
        return redirect(url_for('view_flat_details', flat_id=flat_id, area_tolerance=area_tolerance))

    return render_template('estimate_flat.html', default_area_tolerance=10.0, flat_id='')


def calculate_investment_analysis(flat_data, similar_rentals, similar_sales, area_tolerance):
    """
    Calculate investment analysis for a flat.

    :param flat_data: dict, flat information
    :param similar_rentals: list, similar rental flats
    :param similar_sales: list, similar sales flats
    :param area_tolerance: float, area tolerance used
    :return: dict, investment analysis data
    """
    # Simple investment calculation
    rental_median = 0
    annual_rental_income = 0
    sales_avg = 0
    yield_percentage = 0

    # Calculate rental median (monthly rent)
    if similar_rentals and len(similar_rentals) > 0:
        rental_prices = [flat.get('price', 0) if isinstance(flat, dict) else getattr(flat, 'price', 0) for flat in
                         similar_rentals]
        rental_prices = [p for p in rental_prices if p > 0]  # Filter out 0 prices
        if rental_prices:
            sorted_rental_prices = sorted(rental_prices)
            n = len(sorted_rental_prices)
            if n % 2 == 0:
                rental_median = (sorted_rental_prices[n // 2 - 1] + sorted_rental_prices[n // 2]) / 2
            else:
                rental_median = sorted_rental_prices[n // 2]

            # Expected Annual Rent = 12 × median monthly rent
            annual_rental_income = rental_median * 12

    # Calculate sales average
    if similar_sales and len(similar_sales) > 0:
        sales_prices = [flat.get('price', 0) if isinstance(flat, dict) else getattr(flat, 'price', 0) for flat in
                        similar_sales]
        sales_prices = [p for p in sales_prices if p > 0]  # Filter out 0 prices
        if sales_prices:
            sales_avg = sum(sales_prices) / len(sales_prices)

    # Calculate yield: Expected Annual Rent / Sale price (current flat's price)
    current_price = flat_data.get('price', 0) if isinstance(flat_data, dict) else getattr(flat_data, 'price', 0)
    if current_price > 0 and annual_rental_income > 0:
        yield_percentage = (annual_rental_income / current_price) * 100

    return {
        'rental_median': rental_median,
        'sales_avg': sales_avg,
        'annual_rental_income': annual_rental_income,
        'yield_percentage': yield_percentage
    }


@app.route('/favorites')
def favorites(db_path='flats.db'):
    """Display user's favorite flats."""
    db = OrthancDB(db_path)
    favorites_list = db.get_favorites()
    db.disconnect()

    return render_template('favorites.html', favorites=favorites_list)


@app.route('/api/favorites/add', methods=['POST'])
def add_to_favorites(db_path='flats.db'):
    """API endpoint to add a flat to favorites."""
    data = request.get_json()
    flat_id = data.get('flat_id')
    flat_type = data.get('flat_type')
    flat_data = data.get('flat_data')

    if not flat_id or not flat_type:
        raise Exception("Missing flat_id or flat_type")

    db = OrthancDB(db_path)
    success = db.add_to_favorites(flat_data, flat_type)
    db.disconnect()

    if not success:
        raise Exception("Failed to add to favorites")

    return jsonify({'success': True, 'message': 'Added to favorites'})


@app.route('/api/favorites/remove', methods=['POST'])
def remove_from_favorites(db_path='flats.db'):
    """API endpoint to remove a flat from favorites."""
    data = request.get_json()
    flat_id = data.get('flat_id')
    flat_type = data.get('flat_type')

    if not flat_id or not flat_type:
        raise Exception("Missing flat_id or flat_type")

    db = OrthancDB(db_path)
    success = db.remove_from_favorites(flat_id, flat_type)
    db.disconnect()

    if not success:
        raise Exception("Failed to remove from favorites")

    return jsonify({'success': True, 'message': 'Removed from favorites'})


@app.route('/api/favorites/check', methods=['POST'])
def check_favorite_status(db_path='flats.db'):
    """API endpoint to check if a flat is in favorites."""
    data = request.get_json()
    flat_id = data.get('flat_id')
    flat_type = data.get('flat_type')

    if not flat_id or not flat_type:
        raise Exception("Missing flat_id or flat_type")

    db = OrthancDB(db_path)
    is_favorite = db.is_favorite(flat_id, flat_type)
    db.disconnect()

    return jsonify({'success': True, 'is_favorite': is_favorite})


@app.route('/api/favorites/check-batch', methods=['POST'])
def check_favorite_status_batch(db_path='flats.db'):
    """API endpoint to check multiple flats' favorite status in one call."""
    data = request.get_json()
    flats = data.get('flats', [])

    if not flats:
        raise Exception("No flats provided")

    db = OrthancDB(db_path)
    results = {}
    for flat in flats:
        flat_id = flat.get('flat_id')
        flat_type = flat.get('flat_type')

        if flat_id and flat_type:
            is_favorite = db.is_favorite(flat_id, flat_type)
            results[flat_id] = is_favorite
        else:
            results[flat_id] = False

    db.disconnect()
    return jsonify({'success': True, 'results': results})


@app.route('/api/complexes')
def api_complexes():
    """API endpoint to get all complexes for autocomplete."""
    complexes = api_client.get_all_complexes()
    return jsonify({'complexes': complexes})


@app.route('/api/exchange_rates')
def api_exchange_rates():
    """API endpoint to get current exchange rates."""
    # Get rates from database first
    db = OrthancDB()
    eur_rate = db.get_latest_rate('EUR')
    usd_rate = db.get_latest_rate('USD')
    db.disconnect()

    # If no rates in database, fetch from web
    if not eur_rate or not usd_rate:
        from price.src.currency import CurrencyManager
        currency_manager = CurrencyManager()
        rates = currency_manager.fetch_mig_exchange_rates()

        if rates:
            eur_rate = rates['EUR']
            usd_rate = rates['USD']
        else:
            raise Exception("Failed to fetch exchange rates from web")

    return jsonify({
        'EUR': eur_rate,
        'USD': usd_rate
    })


@app.route('/toggle_currency')
def toggle_currency():
    """Toggle currency preference and redirect back."""
    show_eur = request.args.get('eur', 'false') == 'true'
    response = redirect(request.referrer or url_for('index'))
    response.set_cookie('showEur', str(show_eur).lower())
    return response


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
