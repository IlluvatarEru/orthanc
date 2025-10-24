#!/usr/bin/env python3
"""
Clean, simple Flask webapp that fails fast and never silently catches errors.
"""

from flask import Flask, render_template, request, jsonify, redirect, url_for, flash
from datetime import datetime
import logging
from urllib.parse import unquote

from api.src.analysis_objects import SalesAnalysisResponse, FlatTypeStats, Opportunity
from common.src.flat_info import FlatInfo
# Import our modules
from db.src.write_read_database import OrthancDB
from frontend.src.webapp_api_client import WebappAPIClient
from scrapers.src.krisha_rental_scraping import scrape_and_save_jk_rentals
from scrapers.src.krisha_sales_scraping import scrape_and_save_jk_sales
from scrapers.src.residential_complex_scraper import search_complex_by_name

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize Flask app
app = Flask(__name__, template_folder='../templates', static_folder='../static')
app.secret_key = 'your-secret-key-here'

# Initialize API client
api_client = WebappAPIClient()


def calculate_flat_type_overall_stats(analysis_by_flat_type):
    """
    Calculate overall statistics from flat type analysis for template display.
    
    :param analysis_by_flat_type: dict, flat type analysis data
    :return: dict, overall statistics
    """
    if not analysis_by_flat_type:
        return {
            'median_yield': None,
            'mean_yield': None,
            'yield_range': None,
            'valid_flat_types_count': 0
        }

    # Get valid flat types (those with both rental and sales data)
    valid_flat_types = []
    for flat_type_data in analysis_by_flat_type.values():
        if (flat_type_data.get('rental_count', 0) > 0 and
                flat_type_data.get('sales_count', 0) > 0 and
                flat_type_data.get('rental_stats', {}).get('mean_yield', 0) > 0):
            valid_flat_types.append(flat_type_data)

    if not valid_flat_types:
        return {
            'median_yield': None,
            'mean_yield': None,
            'yield_range': None,
            'valid_flat_types_count': 0
        }

    # Calculate statistics
    yields = [flat_type['rental_stats']['mean_yield'] for flat_type in valid_flat_types]
    yield_mins = [flat_type['rental_stats']['min_yield'] for flat_type in valid_flat_types]
    yield_maxs = [flat_type['rental_stats']['max_yield'] for flat_type in valid_flat_types]

    # Mean yield
    mean_yield = sum(yields) / len(yields)

    # Median yield
    sorted_yields = sorted(yields)
    if len(sorted_yields) % 2 == 0:
        median_yield = (sorted_yields[len(sorted_yields) // 2 - 1] + sorted_yields[len(sorted_yields) // 2]) / 2
    else:
        median_yield = sorted_yields[len(sorted_yields) // 2]

    # Yield range
    min_yield = min(yield_mins)
    max_yield = max(yield_maxs)

    return {
        'median_yield': median_yield,
        'mean_yield': mean_yield,
        'yield_range': (min_yield, max_yield),
        'valid_flat_types_count': len(valid_flat_types)
    }


@app.route('/')
def index():
    """Dashboard home page."""
    stats_result = api_client.get_database_stats()

    if not stats_result.get("success"):
        raise Exception(f"Database stats API failed: {stats_result.get('error', 'Unknown error')}")

    stats = stats_result.get("stats", {})
    return render_template('index.html',
                           total_flats=stats.get('total_flats', 0),
                           rental_flats=stats.get('rental_flats', 0),
                           sales_flats=stats.get('sales_flats', 0),
                           complexes=stats.get('complexes', 0),
                           new_flats=stats.get('new_flats', 0),
                           new_rentals=stats.get('new_rentals', 0),
                           new_sales=stats.get('new_sales', 0),
                           new_complexes=stats.get('new_complexes', 0))


@app.route('/search_jk', methods=['GET', 'POST'])
def search_jk():
    """Search for residential complexes."""
    if request.method == 'POST':
        search_term = request.form.get('residential_complex_name', '').strip()
        if not search_term:
            raise Exception("Search term is required")

        result = api_client.search_complexes(search_term)

        if not result.get("success"):
            raise Exception(f"Search API failed: {result.get('error', 'Unknown error')}")

        complexes = result.get("complexes", [])
        deduplication_info = result.get("deduplication_info")

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
        complex_id = complex_info.get('complex_id')
        scrape_result = api_client.scrape_complex_data(residential_complex_name, complex_id)

        if not scrape_result.get("success"):
            raise Exception(
                f"Failed to scrape data for {residential_complex_name}: {scrape_result.get('error', 'Unknown error')}")

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
    if not refresh_result.get("success"):
        raise Exception(f"Refresh analysis API failed: {refresh_result.get('error', 'Unknown error')}")

    return jsonify({
        'success': True,
        'message': f'Successfully refreshed analysis for {residential_complex_name}'
    })


@app.route('/estimate_flat', methods=['GET', 'POST'])
def estimate_flat():
    """Estimate flat investment potential."""
    if request.method == 'POST':
        flat_id = request.form.get('flat_id', '').strip()
        area_tolerance = float(request.form.get('area_tolerance', 10.0))

        if not flat_id:
            raise Exception("Flat ID is required")

        # Get flat information using API
        flat_data = api_client.get_flat_info(flat_id)
        if not flat_data:
            raise Exception(f"Flat {flat_id} not found")

        # Get similar flats using API
        similar_result = api_client.get_similar_flats(flat_id, area_tolerance, 3)
        if not similar_result.get("success"):
            raise Exception(f"Failed to get similar flats: {similar_result.get('error', 'Unknown error')}")

        similar_rentals = similar_result.get("similar_rentals", [])
        similar_sales = similar_result.get("similar_sales", [])

        if not similar_rentals or not similar_sales:
            raise Exception(
                f"Insufficient data for analysis. Found {len(similar_rentals)} rental and {len(similar_sales)} sales flats.")

        # Calculate investment analysis
        return calculate_investment_analysis(flat_data, similar_rentals, similar_sales, area_tolerance)

    return render_template('estimate_flat.html', default_area_tolerance=10.0, flat_id='')


def calculate_investment_analysis(flat_data, similar_rentals, similar_sales, area_tolerance):
    """Calculate investment analysis for a flat."""
    # Simple investment calculation
    rental_avg = sum(flat['price'] for flat in similar_rentals) / len(similar_rentals)
    sales_avg = sum(flat['price'] for flat in similar_sales) / len(similar_sales)

    annual_rental_income = rental_avg * 12
    yield_percentage = (annual_rental_income / sales_avg) * 100

    return render_template('estimate_result.html',
                           flat_data=flat_data,
                           similar_rentals=similar_rentals,
                           similar_sales=similar_sales,
                           rental_avg=rental_avg,
                           sales_avg=sales_avg,
                           yield_percentage=yield_percentage,
                           area_tolerance=area_tolerance)


@app.route('/similar_flats/<flat_type>/<residential_complex_name>')
def view_similar_flats(flat_type, residential_complex_name, db_path='flats.db'):
    """Display similar flats for a specific complex and type."""
    # Get complex information
    complex_info = search_complex_by_name(residential_complex_name)
    if not complex_info:
        raise Exception(f"Complex '{residential_complex_name}' not found")

    # Get flats by type
    db = OrthancDB(db_path)
    flats = db.get_flats_for_residential_complex(residential_complex_name, flat_type)
    db.disconnect()

    # If no flats found, try to scrape data
    if not flats:
        if flat_type == 'rental':
            saved_count = scrape_and_save_jk_rentals(residential_complex_name, max_pages=10, db_path=db_path)
        elif flat_type == 'sales':
            saved_count = scrape_and_save_jk_sales(residential_complex_name, max_pages=10, db_path=db_path)
        else:
            rental_count = scrape_and_save_jk_rentals(residential_complex_name, max_pages=10, db_path=db_path)
            sales_count = scrape_and_save_jk_sales(residential_complex_name, max_pages=10, db_path=db_path)
            saved_count = rental_count + sales_count

        if saved_count == 0:
            raise Exception(f"No new flats found for {residential_complex_name}")

        # Get flats again after scraping
        db = OrthancDB(db_path)
        flats = db.get_flats_for_residential_complex(residential_complex_name, flat_type)
        db.disconnect()

    # Calculate median price
    flat_prices = [flat.price for flat in flats] if flats else []
    flat_median = sorted(flat_prices)[len(flat_prices) // 2] if flat_prices else 0

    return render_template('view_similar_flats.html',
                           complex_info=complex_info,
                           flats=flats,
                           flat_type=flat_type,
                           flat_median=flat_median)


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


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
