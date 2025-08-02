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
    
    def get_jk_rental_stats(self, complex_name: str, area_max: float = 35.0, 
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
    
    def get_jk_sales_stats(self, complex_name: str, area_max: float = 35.0,
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
    
    def calculate_rental_yield(self, complex_name: str, area_max: float = 35.0,
                             query_date: Optional[str] = None) -> Dict:
        """
        Calculate rental yield for a residential complex.
        
        :param complex_name: str, name of the residential complex
        :param area_max: float, maximum area in square meters
        :param query_date: Optional[str], specific query date (YYYY-MM-DD), if None uses latest
        :return: Dict, rental yield analysis
        """
        if query_date is None:
            query_date = datetime.now().strftime('%Y-%m-%d')
        
        # Get rental and sales statistics
        rental_stats = self.get_jk_rental_stats(complex_name, area_max, query_date)
        sales_stats = self.get_jk_sales_stats(complex_name, area_max, query_date)
        
        # Check if we have both rental and sales data
        if rental_stats.get('error') or sales_stats.get('error'):
            return {
                'complex_name': complex_name,
                'query_date': query_date,
                'area_max': area_max,
                'error': 'Missing rental or sales data',
                'rental_count': rental_stats.get('count', 0),
                'sales_count': sales_stats.get('count', 0)
            }
        
        # Calculate yields
        rental_median = rental_stats['price_stats']['median']
        sales_median = sales_stats['price_stats']['median']
        rental_avg = rental_stats['price_stats']['avg']
        sales_avg = sales_stats['price_stats']['avg']
        
        # Annual rental yield calculations
        median_yield = (rental_median * 12) / sales_median if sales_median > 0 else 0
        avg_yield = (rental_avg * 12) / sales_avg if sales_avg > 0 else 0
        
        return {
            'complex_name': complex_name,
            'query_date': query_date,
            'area_max': area_max,
            'rental_stats': rental_stats,
            'sales_stats': sales_stats,
            'yield_analysis': {
                'median_rental_price': rental_median,
                'median_sale_price': sales_median,
                'median_annual_rent': rental_median * 12,
                'median_yield': median_yield,
                'median_yield_percent': median_yield * 100,
                'avg_rental_price': rental_avg,
                'avg_sale_price': sales_avg,
                'avg_annual_rent': rental_avg * 12,
                'avg_yield': avg_yield,
                'avg_yield_percent': avg_yield * 100
            }
        }
    
    def get_jk_comprehensive_analysis(self, complex_name: str, area_max: float = 35.0,
                                    query_date: Optional[str] = None) -> Dict:
        """
        Get comprehensive analysis for a residential complex.
        
        :param complex_name: str, name of the residential complex
        :param area_max: float, maximum area in square meters
        :param query_date: Optional[str], specific query date (YYYY-MM-DD), if None uses latest
        :return: Dict, comprehensive analysis
        """
        yield_analysis = self.calculate_rental_yield(complex_name, area_max, query_date)
        
        if 'error' in yield_analysis:
            return yield_analysis
        
        # Add market insights
        rental_stats = yield_analysis['rental_stats']
        sales_stats = yield_analysis['sales_stats']
        yield_data = yield_analysis['yield_analysis']
        
        # Calculate price per square meter
        rental_price_per_sqm = rental_stats['price_stats']['avg'] / rental_stats['area_stats']['avg']
        sales_price_per_sqm = sales_stats['price_stats']['avg'] / sales_stats['area_stats']['avg']
        
        # Market insights
        insights = {
            'price_per_sqm': {
                'rental': rental_price_per_sqm,
                'sales': sales_price_per_sqm,
                'ratio': rental_price_per_sqm / sales_price_per_sqm if sales_price_per_sqm > 0 else 0
            },
            'market_position': {
                'rental_competitiveness': 'High' if yield_data['median_yield_percent'] > 8 else 'Medium' if yield_data['median_yield_percent'] > 5 else 'Low',
                'investment_potential': 'High' if yield_data['median_yield_percent'] > 7 else 'Medium' if yield_data['median_yield_percent'] > 5 else 'Low'
            },
            'data_quality': {
                'rental_sample_size': rental_stats['count'],
                'sales_sample_size': sales_stats['count'],
                'reliability': 'High' if min(rental_stats['count'], sales_stats['count']) >= 5 else 'Medium' if min(rental_stats['count'], sales_stats['count']) >= 3 else 'Low'
            }
        }
        
        yield_analysis['insights'] = insights
        return yield_analysis
    
    def print_jk_analysis(self, complex_name: str, area_max: float = 35.0,
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
        
        # Yield analysis
        yield_data = analysis['yield_analysis']
        print(f"\n📈 Rental Yield Analysis:")
        print(f"   Median annual rent: {yield_data['median_annual_rent']:,.0f} tenge")
        print(f"   Median sale price: {yield_data['median_sale_price']:,.0f} tenge")
        print(f"   Median yield: {yield_data['median_yield_percent']:.2f}%")
        print(f"   Average yield: {yield_data['avg_yield_percent']:.2f}%")
        
        # Market insights
        insights = analysis['insights']
        print(f"\n💡 Market Insights:")
        print(f"   Rental price per m²: {insights['price_per_sqm']['rental']:,.0f} tenge")
        print(f"   Sales price per m²: {insights['price_per_sqm']['sales']:,.0f} tenge")
        print(f"   Investment potential: {insights['market_position']['investment_potential']}")
        print(f"   Data reliability: {insights['data_quality']['reliability']}")


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
    yield_data = analytics.calculate_rental_yield("Meridian", area_max=35.0)
    if 'error' not in yield_data:
        print(f"   Median yield: {yield_data['yield_analysis']['median_yield_percent']:.2f}%")


if __name__ == "__main__":
    main() 