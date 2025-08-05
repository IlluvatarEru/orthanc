"""
JK (Residential Complex) Analytics Module

This module provides functions to analyze residential complex data
including rental yields, price statistics, and market analysis.
"""

import sqlite3
import statistics
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timedelta
from db.enhanced_database import EnhancedFlatDatabase


class JKAnalytics:
    """
    Analytics class for residential complex (JK) data analysis.
    """
    
    def __init__(self, db_path: str = "flats.db"):
        """
        Initialize analytics with database connection.
        
        :param db_path: str, path to database file
        """
        self.db_path = db_path
        self.db = EnhancedFlatDatabase(db_path)
    
    def get_jk_rental_stats(self, complex_name: str, area_max: float = 100.0, 
                           query_date: Optional[str] = None) -> Dict:
        """
        Get rental statistics for a specific residential complex.
        
        :param complex_name: str, name of the residential complex
        :param area_max: float, maximum area in square meters
        :param query_date: Optional[str], specific query date (YYYY-MM-DD), if None uses latest
        :return: Dict, rental statistics
        """
        if query_date is None:
            query_date = datetime.now().strftime('%Y-%m-%d')
        
        self.db.connect()
        
        try:
            # Get rental data for the complex - use DISTINCT to avoid duplicates and get historical data
            cursor = self.db.conn.execute("""
                SELECT DISTINCT flat_id, price, area, floor, total_floors, construction_year
                FROM rental_flats 
                WHERE residential_complex LIKE ? 
                AND area <= ? 
                ORDER BY flat_id, query_date DESC
            """, (f'%{complex_name}%', area_max))
            
            # Group by flat_id to get only the most recent entry for each flat
            rental_data = {}
            for row in cursor.fetchall():
                flat_id = row[0]
                if flat_id not in rental_data:
                    rental_data[flat_id] = row[1:]  # Store price, area, floor, total_floors, construction_year
            
            flats = list(rental_data.values())
            
            if not flats:
                return {
                    'complex_name': complex_name,
                    'query_date': query_date,
                    'area_max': area_max,
                    'count': 0,
                    'error': 'No rental data found'
                }
            
            # Extract data
            prices = [flat[0] for flat in flats]
            areas = [flat[1] for flat in flats]
            floors = [flat[2] for flat in flats if flat[2] is not None]
            total_floors = [flat[3] for flat in flats if flat[3] is not None]
            construction_years = [flat[4] for flat in flats if flat[4] is not None]
            
            # Calculate statistics
            stats = {
                'complex_name': complex_name,
                'query_date': query_date,
                'area_max': area_max,
                'count': len(flats),
                'price_stats': {
                    'min': min(prices),
                    'max': max(prices),
                    'avg': sum(prices) / len(prices),
                    'median': statistics.median(prices),
                    'std_dev': statistics.stdev(prices) if len(prices) > 1 else 0
                },
                'area_stats': {
                    'min': min(areas),
                    'max': max(areas),
                    'avg': sum(areas) / len(areas),
                    'median': statistics.median(areas)
                }
            }
            
            # Add floor statistics if available
            if floors:
                stats['floor_stats'] = {
                    'min': min(floors),
                    'max': max(floors),
                    'avg': sum(floors) / len(floors)
                }
            
            if total_floors:
                stats['total_floors_stats'] = {
                    'min': min(total_floors),
                    'max': max(total_floors),
                    'avg': sum(total_floors) / len(total_floors)
                }
            
            if construction_years:
                stats['construction_year_stats'] = {
                    'min': min(construction_years),
                    'max': max(construction_years),
                    'avg': sum(construction_years) / len(construction_years)
                }
            
            return stats
            
        finally:
            self.db.disconnect()
    
    def get_jk_sales_stats(self, complex_name: str, area_max: float = 100.0,
                          query_date: Optional[str] = None) -> Dict:
        """
        Get sales statistics for a specific residential complex.
        
        :param complex_name: str, name of the residential complex
        :param area_max: float, maximum area in square meters
        :param query_date: Optional[str], specific query date (YYYY-MM-DD), if None uses latest
        :return: Dict, sales statistics
        """
        if query_date is None:
            query_date = datetime.now().strftime('%Y-%m-%d')
        
        self.db.connect()
        
        try:
            # Get sales data for the complex - use DISTINCT to avoid duplicates and get historical data
            cursor = self.db.conn.execute("""
                SELECT DISTINCT flat_id, price, area, floor, total_floors, construction_year
                FROM sales_flats 
                WHERE residential_complex LIKE ? 
                AND area <= ? 
                ORDER BY flat_id, query_date DESC
            """, (f'%{complex_name}%', area_max))
            
            # Group by flat_id to get only the most recent entry for each flat
            sales_data = {}
            for row in cursor.fetchall():
                flat_id = row[0]
                if flat_id not in sales_data:
                    sales_data[flat_id] = row[1:]  # Store price, area, floor, total_floors, construction_year
            
            flats = list(sales_data.values())
            
            if not flats:
                return {
                    'complex_name': complex_name,
                    'query_date': query_date,
                    'area_max': area_max,
                    'count': 0,
                    'error': 'No sales data found'
                }
            
            # Extract data
            prices = [flat[0] for flat in flats]
            areas = [flat[1] for flat in flats]
            floors = [flat[2] for flat in flats if flat[2] is not None]
            total_floors = [flat[3] for flat in flats if flat[3] is not None]
            construction_years = [flat[4] for flat in flats if flat[4] is not None]
            
            # Calculate statistics
            stats = {
                'complex_name': complex_name,
                'query_date': query_date,
                'area_max': area_max,
                'count': len(flats),
                'price_stats': {
                    'min': min(prices),
                    'max': max(prices),
                    'avg': sum(prices) / len(prices),
                    'median': statistics.median(prices),
                    'std_dev': statistics.stdev(prices) if len(prices) > 1 else 0
                },
                'area_stats': {
                    'min': min(areas),
                    'max': max(areas),
                    'avg': sum(areas) / len(areas),
                    'median': statistics.median(areas)
                }
            }
            
            # Add floor statistics if available
            if floors:
                stats['floor_stats'] = {
                    'min': min(floors),
                    'max': max(floors),
                    'avg': sum(floors) / len(floors)
                }
            
            if total_floors:
                stats['total_floors_stats'] = {
                    'min': min(total_floors),
                    'max': max(total_floors),
                    'avg': sum(total_floors) / len(total_floors)
                }
            
            if construction_years:
                stats['construction_year_stats'] = {
                    'min': min(construction_years),
                    'max': max(construction_years),
                    'avg': sum(construction_years) / len(construction_years)
                }
            
            return stats
            
        finally:
            self.db.disconnect()
    
    def get_jk_comprehensive_analysis(self, complex_name: str, area_max: float = 100.0,
                                    query_date: Optional[str] = None) -> Dict:
        """
        Get comprehensive analysis for a residential complex.
        
        :param complex_name: str, name of the residential complex
        :param area_max: float, maximum area in square meters
        :param query_date: Optional[str], specific query date (YYYY-MM-DD), if None uses latest
        :return: Dict, comprehensive analysis
        """
        if query_date is None:
            query_date = datetime.now().strftime('%Y-%m-%d')
        
        # Get rental and sales statistics
        rental_stats = self.get_jk_rental_stats(complex_name, area_max, query_date)
        sales_stats = self.get_jk_sales_stats(complex_name, area_max, query_date)
        
        # Check if we have at least some data to work with
        rental_count = rental_stats.get('count', 0)
        sales_count = sales_stats.get('count', 0)
        
        if rental_count == 0 and sales_count == 0:
            return {
                'complex_name': complex_name,
                'query_date': query_date,
                'area_max': area_max,
                'error': 'No data available for analysis',
                'rental_count': 0,
                'sales_count': 0
            }
        
        # If we have data but some stats have errors, create partial analysis
        if 'error' in rental_stats or 'error' in sales_stats:
            # Create a basic analysis with available data
            analysis_result = {
                'complex_name': complex_name,
                'query_date': query_date,
                'area_max': area_max,
                'rental_count': rental_count,
                'sales_count': sales_count
            }
            
            # Add available stats (only if no error)
            if 'error' not in rental_stats:
                analysis_result['rental_stats'] = rental_stats
            else:
                analysis_result['rental_stats'] = {'count': 0, 'error': rental_stats.get('error', 'No rental data')}
                
            if 'error' not in sales_stats:
                analysis_result['sales_stats'] = sales_stats
            else:
                analysis_result['sales_stats'] = {'count': 0, 'error': sales_stats.get('error', 'No sales data')}
            
            return analysis_result
        
        # Add market insights (only if both types of data are available)
        rental_data = rental_stats
        sales_data = sales_stats
        
        insights = {
            'price_per_sqm': {
                'rental': None,
                'sales': None
            },
            'market_position': {
                'rental_competitiveness': 'N/A',
                'investment_potential': 'N/A'
            },
            'data_quality': {
                'rental_sample_size': rental_data.get('count', 0),
                'sales_sample_size': sales_data.get('count', 0),
                'reliability': 'Low'
            }
        }
        
        # Calculate insights only if both data types are available
        if 'error' not in rental_data and 'error' not in sales_data:
            # Calculate price per square meter
            rental_price_per_sqm = rental_data['price_stats']['avg'] / rental_data['area_stats']['avg']
            sales_price_per_sqm = sales_data['price_stats']['avg'] / sales_data['area_stats']['avg']
            
            insights['price_per_sqm']['rental'] = rental_price_per_sqm
            insights['price_per_sqm']['sales'] = sales_price_per_sqm
            
            insights['market_position']['rental_competitiveness'] = 'High' if rental_price_per_sqm > 100000 else 'Medium' if rental_price_per_sqm > 50000 else 'Low'
            insights['market_position']['investment_potential'] = 'High' if sales_price_per_sqm > 150000 else 'Medium' if sales_price_per_sqm > 80000 else 'Low'
            
            insights['data_quality']['reliability'] = 'High' if min(rental_data['count'], sales_data['count']) >= 5 else 'Medium' if min(rental_data['count'], sales_data['count']) >= 3 else 'Low'
        
        return {
            'complex_name': complex_name,
            'query_date': query_date,
            'area_max': area_max,
            'rental_stats': rental_data,
            'sales_stats': sales_data,
            'insights': insights
        }
    
    def print_jk_analysis(self, complex_name: str, area_max: float = 100.0,
                         query_date: Optional[str] = None) -> None:
        """
        Print formatted analysis for a residential complex.
        
        :param complex_name: str, name of the residential complex
        :param area_max: float, maximum area in square meters
        :param query_date: Optional[str], specific query date (YYYY-MM-DD), if None uses latest
        """
        analysis = self.get_jk_comprehensive_analysis(complex_name, area_max, query_date)
        
        if 'error' in analysis:
            print(f"❌ Error analyzing {complex_name}: {analysis['error']}")
            return
        
        print(f"🏢 Analysis for {analysis['complex_name']}")
        print(f"   Date: {analysis['query_date']}")
        print(f"   Area limit: ≤{analysis['area_max']} m²")
        print("=" * 60)
        
        # Rental statistics
        rental = analysis['rental_stats']
        print(f"📊 Rental Statistics ({rental['count']} flats):")
        print(f"   Price range: {rental['price_stats']['min']:,} - {rental['price_stats']['max']:,} tenge")
        print(f"   Average price: {rental['price_stats']['avg']:,.0f} tenge")
        print(f"   Median price: {rental['price_stats']['median']:,.0f} tenge")
        print(f"   Area range: {rental['area_stats']['min']:.1f} - {rental['area_stats']['max']:.1f} m²")
        print(f"   Average area: {rental['area_stats']['avg']:.1f} m²")
        
        # Sales statistics
        sales = analysis['sales_stats']
        print(f"\n💰 Sales Statistics ({sales['count']} flats):")
        print(f"   Price range: {sales['price_stats']['min']:,} - {sales['price_stats']['max']:,} tenge")
        print(f"   Average price: {sales['price_stats']['avg']:,.0f} tenge")
        print(f"   Median price: {sales['price_stats']['median']:,.0f} tenge")
        print(f"   Area range: {sales['area_stats']['min']:.1f} - {sales['area_stats']['max']:.1f} m²")
        print(f"   Average area: {sales['area_stats']['avg']:.1f} m²")
        
        # Market insights
        insights = analysis['insights']
        print(f"\n💡 Market Insights:")
        print(f"   Rental price per m²: {insights['price_per_sqm']['rental']:,.0f} tenge")
        print(f"   Sales price per m²: {insights['price_per_sqm']['sales']:,.0f} tenge")
        print(f"   Investment potential: {insights['market_position']['investment_potential']}")
        print(f"   Data reliability: {insights['data_quality']['reliability']}")
        
        # You can also get raw data for further analysis
        print(f"\n📋 Raw Data Example:")
        yield_data = analytics.get_jk_comprehensive_analysis("Meridian", area_max=35.0)
        if 'error' not in yield_data:
            print(f"   Rental sample size: {yield_data['insights']['data_quality']['rental_sample_size']}")
            print(f"   Sales sample size: {yield_data['insights']['data_quality']['sales_sample_size']}")

    def get_bucket_analysis(self, complex_name: str, area_max: float = 100.0,
                           query_date: Optional[str] = None) -> Dict:
        """
        Get bucket-based analysis for a residential complex.
        
        Groups flats by room count and area ranges to provide accurate yield analysis.
        
        :param complex_name: str, name of the residential complex
        :param area_max: float, maximum area in square meters
        :param query_date: Optional[str], specific query date (YYYY-MM-DD), if None uses latest
        :return: Dict, bucket-based analysis
        """
        if query_date is None:
            query_date = datetime.now().strftime('%Y-%m-%d')
        
        self.db.connect()
        
        try:
            # Get all rental and sales data for the complex
            rental_cursor = self.db.conn.execute("""
                SELECT DISTINCT flat_id, price, area, floor, total_floors, construction_year
                FROM rental_flats 
                WHERE residential_complex LIKE ? 
                AND area <= ? 
                ORDER BY flat_id, query_date DESC
            """, (f'%{complex_name}%', area_max))
            
            sales_cursor = self.db.conn.execute("""
                SELECT DISTINCT flat_id, price, area, floor, total_floors, construction_year
                FROM sales_flats 
                WHERE residential_complex LIKE ? 
                AND area <= ? 
                ORDER BY flat_id, query_date DESC
            """, (f'%{complex_name}%', area_max))
            
            # Group by flat_id to get only the most recent entry for each flat
            rental_data = {}
            for row in rental_cursor.fetchall():
                flat_id = row[0]
                if flat_id not in rental_data:
                    rental_data[flat_id] = row[1:]  # Store price, area, floor, total_floors, construction_year
            
            sales_data = {}
            for row in sales_cursor.fetchall():
                flat_id = row[0]
                if flat_id not in sales_data:
                    sales_data[flat_id] = row[1:]  # Store price, area, floor, total_floors, construction_year
            
            # Define area buckets based on typical flat sizes
            area_buckets = [
                (0, 30, "Small (≤30m²)"),
                (30, 50, "Medium (30-50m²)"),
                (50, 70, "Large (50-70m²)"),
                (70, 90, "Extra Large (70-90m²)"),
                (90, float('inf'), "Huge (≥90m²)")
            ]
            
            # Define room count buckets (estimated based on area)
            def estimate_rooms(area):
                if area <= 30:
                    return 1
                elif area <= 50:
                    return 2
                elif area <= 70:
                    return 3
                elif area <= 90:
                    return 4
                else:
                    return 5
            
            # Group data by buckets
            bucket_analysis = {}
            
            # Process rental data
            for flat_id, (price, area, floor, total_floors, construction_year) in rental_data.items():
                rooms = estimate_rooms(area)
                
                # Find area bucket
                area_bucket = None
                for min_area, max_area, bucket_name in area_buckets:
                    if min_area <= area < max_area:
                        area_bucket = bucket_name
                        break
                
                if area_bucket:
                    bucket_key = f"{rooms}BR_{area_bucket}"
                    if bucket_key not in bucket_analysis:
                        bucket_analysis[bucket_key] = {
                            'rooms': rooms,
                            'area_bucket': area_bucket,
                            'rental_flats': [],
                            'sales_flats': [],
                            'rental_prices': [],
                            'sales_prices': [],
                            'rental_areas': [],
                            'sales_areas': []
                        }
                    
                    bucket_analysis[bucket_key]['rental_flats'].append({
                        'flat_id': flat_id,
                        'price': price,
                        'area': area,
                        'floor': floor,
                        'total_floors': total_floors,
                        'construction_year': construction_year
                    })
                    bucket_analysis[bucket_key]['rental_prices'].append(price)
                    bucket_analysis[bucket_key]['rental_areas'].append(area)
            
            # Process sales data
            for flat_id, (price, area, floor, total_floors, construction_year) in sales_data.items():
                rooms = estimate_rooms(area)
                
                # Find area bucket
                area_bucket = None
                for min_area, max_area, bucket_name in area_buckets:
                    if min_area <= area < max_area:
                        area_bucket = bucket_name
                        break
                
                if area_bucket:
                    bucket_key = f"{rooms}BR_{area_bucket}"
                    if bucket_key not in bucket_analysis:
                        bucket_analysis[bucket_key] = {
                            'rooms': rooms,
                            'area_bucket': area_bucket,
                            'rental_flats': [],
                            'sales_flats': [],
                            'rental_prices': [],
                            'sales_prices': [],
                            'rental_areas': [],
                            'sales_areas': []
                        }
                    
                    bucket_analysis[bucket_key]['sales_flats'].append({
                        'flat_id': flat_id,
                        'price': price,
                        'area': area,
                        'floor': floor,
                        'total_floors': total_floors,
                        'construction_year': construction_year
                    })
                    bucket_analysis[bucket_key]['sales_prices'].append(price)
                    bucket_analysis[bucket_key]['sales_areas'].append(area)
            
            # Calculate yield analysis for each bucket
            bucket_yields = []
            total_rental_count = 0
            total_sales_count = 0
            all_yields = []  # For overall statistics
            
            for bucket_key, bucket_data in bucket_analysis.items():
                rental_count = len(bucket_data['rental_flats'])
                sales_count = len(bucket_data['sales_flats'])
                total_rental_count += rental_count
                total_sales_count += sales_count
                
                if rental_count > 0 and sales_count > 0:
                    # Calculate statistics for this bucket
                    rental_median = statistics.median(bucket_data['rental_prices'])
                    sales_median = statistics.median(bucket_data['sales_prices'])
                    rental_avg = sum(bucket_data['rental_prices']) / len(bucket_data['rental_prices'])
                    sales_avg = sum(bucket_data['sales_prices']) / len(bucket_data['sales_prices'])
                    
                    # Calculate yields for each rental-sales pair
                    bucket_yields_list = []
                    for rental_price in bucket_data['rental_prices']:
                        for sales_price in bucket_data['sales_prices']:
                            if sales_price > 0:
                                yield_rate = (rental_price * 12) / sales_price * 100
                                bucket_yields_list.append(yield_rate)
                    
                    # Calculate yield statistics for this bucket
                    if bucket_yields_list:
                        bucket_yield_min = min(bucket_yields_list)
                        bucket_yield_max = max(bucket_yields_list)
                        bucket_yield_median = statistics.median(bucket_yields_list)
                        bucket_yield_mean = sum(bucket_yields_list) / len(bucket_yields_list)
                        all_yields.extend(bucket_yields_list)
                    else:
                        bucket_yield_min = bucket_yield_max = bucket_yield_median = bucket_yield_mean = 0
                    
                    # Calculate yields
                    median_yield = (rental_median * 12) / sales_median if sales_median > 0 else 0
                    avg_yield = (rental_avg * 12) / sales_avg if sales_avg > 0 else 0
                    
                    bucket_yields.append({
                        'bucket_key': bucket_key,
                        'rooms': bucket_data['rooms'],
                        'area_bucket': bucket_data['area_bucket'],
                        'rental_count': rental_count,
                        'sales_count': sales_count,
                        'rental_median': rental_median,
                        'sales_median': sales_median,
                        'rental_avg': rental_avg,
                        'sales_avg': sales_avg,
                        'median_yield_percent': median_yield * 100,
                        'avg_yield_percent': avg_yield * 100,
                        'yield_min': bucket_yield_min,
                        'yield_max': bucket_yield_max,
                        'yield_median': bucket_yield_median,
                        'yield_mean': bucket_yield_mean,
                        'yields': bucket_yields_list,  # Add the yields array for box plot
                        'rental_areas': bucket_data['rental_areas'],
                        'sales_areas': bucket_data['sales_areas']
                    })
                elif rental_count > 0 or sales_count > 0:
                    # Only one type of data available
                    bucket_yields.append({
                        'bucket_key': bucket_key,
                        'rooms': bucket_data['rooms'],
                        'area_bucket': bucket_data['area_bucket'],
                        'rental_count': rental_count,
                        'sales_count': sales_count,
                        'rental_median': statistics.median(bucket_data['rental_prices']) if rental_count > 0 else 0,
                        'sales_median': statistics.median(bucket_data['sales_prices']) if sales_count > 0 else 0,
                        'rental_avg': sum(bucket_data['rental_prices']) / len(bucket_data['rental_prices']) if rental_count > 0 else 0,
                        'sales_avg': sum(bucket_data['sales_prices']) / len(bucket_data['sales_prices']) if sales_count > 0 else 0,
                        'median_yield_percent': 0,
                        'avg_yield_percent': 0,
                        'yield_min': 0,
                        'yield_max': 0,
                        'yield_median': 0,
                        'yield_mean': 0,
                        'yields': [],  # Empty yields array for incomplete buckets
                        'rental_areas': bucket_data['rental_areas'],
                        'sales_areas': bucket_data['sales_areas']
                    })
            
            # Calculate overall statistics
            valid_buckets = [b for b in bucket_yields if b['rental_count'] > 0 and b['sales_count'] > 0]
            
            if valid_buckets and all_yields:
                overall_median_yield = statistics.median([b['median_yield_percent'] for b in valid_buckets])
                overall_yield_min = min(all_yields)
                overall_yield_max = max(all_yields)
                overall_yield_median = statistics.median(all_yields)
                overall_yield_mean = sum(all_yields) / len(all_yields)
            else:
                overall_median_yield = overall_yield_min = overall_yield_max = overall_yield_median = overall_yield_mean = 0
            
            return {
                'complex_name': complex_name,
                'query_date': query_date,
                'area_max': area_max,
                'bucket_analysis': bucket_yields,
                'overall_stats': {
                    'total_rental_count': total_rental_count,
                    'total_sales_count': total_sales_count,
                    'valid_buckets': len(valid_buckets),
                    'overall_median_yield': overall_median_yield,
                    'overall_yield_min': overall_yield_min,
                    'overall_yield_max': overall_yield_max,
                    'overall_yield_median': overall_yield_median,
                    'overall_yield_mean': overall_yield_mean
                },
                'area_buckets': area_buckets
            }
            
        finally:
            self.db.disconnect()


def main():
    """
    Example usage of JK analytics.
    """
    analytics = JKAnalytics()
    
    # Analyze Meridian Apartments
    print("🔍 JK Analytics Example")
    print("=" * 60)
    
    analytics.print_jk_analysis("Meridian", area_max=35.0)
    
    # You can also get raw data for further analysis
    print(f"\n📋 Raw Data Example:")
    yield_data = analytics.get_jk_comprehensive_analysis("Meridian", area_max=35.0)
    if 'error' not in yield_data:
        print(f"   Rental sample size: {yield_data['insights']['data_quality']['rental_sample_size']}")
        print(f"   Sales sample size: {yield_data['insights']['data_quality']['sales_sample_size']}")


if __name__ == "__main__":
    main() 