"""
JK Analytics Module for Sales Analysis

This module provides comprehensive analytics for residential complex (JK) sales data,
including current market analysis and historical trends.
"""

import sqlite3
import logging
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timedelta
from dataclasses import dataclass

from common.src.flat_info import FlatInfo
from db.src.write_read_database import OrthancDB

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class PriceStats:
    """Price statistics for a flat type bucket."""
    mean: float
    median: float
    min: float
    max: float
    count: int


@dataclass
class StatsForFlatType:
    """
    Historical data point for time series analysis.
    """
    date: str
    flat_type: str
    residential_complex: str
    mean_price: float
    median_price: float
    min_price: float
    max_price: float
    count: int


@dataclass
class FlatOpportunity:
    """
    Represents a good opportunity flat.
    """
    flat_info: FlatInfo
    stats_for_flat_type: StatsForFlatType
    discount_percentage_vs_median: float
    query_date: str


@dataclass
class CurrentMarketAnalysis:
    """Current market analysis for a JK."""
    jk_name: str
    global_stats: PriceStats
    flat_type_buckets: Dict[str, PriceStats]
    # flat_type -> list of opportunities
    opportunities: Dict[str, List[FlatOpportunity]]
    analysis_date: str


@dataclass
class HistoricalAnalysis:
    """Historical analysis for a JK."""
    jk_name: str
    flat_type_timeseries: Dict[str, List[StatsForFlatType]]
    analysis_period: Tuple[str, str]  # (start_date, end_date)


class JKAnalytics:
    """
    Analytics engine for residential complex (JK) sales data.
    
    Provides comprehensive analysis of current market conditions and historical trends.
    """

    def __init__(self, db_path: str = "flats.db"):
        """
        Initialize JK Analytics.
        
        :param db_path: str, path to SQLite database file
        """
        self.db_path = db_path
        self.db = OrthancDB(db_path)

    def analyse_jk_for_sales(self, jk_name: str, sale_discount_percentage: float = 0.15) -> Dict:
        """
        Analyze sales data for a specific residential complex (JK).
        
        :param jk_name: str, name of the residential complex
        :param sale_discount_percentage: float, discount percentage to identify opportunities (default: 15%)
        :return: Dict, comprehensive analysis including current market and historical data
        """
        logger.info(f"Starting analysis for JK: {jk_name}")

        try:
            # Get current market analysis
            current_analysis = self._analyze_current_market(jk_name, sale_discount_percentage)

            # Get historical analysis
            historical_analysis = self._analyze_historical_trends(jk_name)

            return {
                "jk_name": jk_name,
                "analysis_timestamp": datetime.now().isoformat(),
                "sale_discount_percentage": sale_discount_percentage,
                "current_market": current_analysis,
                "historical_analysis": historical_analysis
            }

        except Exception as e:
            logger.error(f"Error analyzing JK {jk_name}: {e}")
            raise

    def _analyze_current_market(self, jk_name: str, discount_percentage: float) -> CurrentMarketAnalysis:
        """
        Analyze current market conditions for a JK.
        
        :param jk_name: str, name of the residential complex
        :param discount_percentage: float, discount percentage for opportunities
        :return: CurrentMarketAnalysis, current market analysis
        """
        logger.info(f"Analyzing current market for {jk_name}")

        self.db.connect()

        # Get latest sales data (most recent query_date for each flat)
        latest_sales_query = """
            SELECT sf.*, 
                   ROW_NUMBER() OVER (PARTITION BY sf.flat_id ORDER BY sf.query_date DESC) as rn
            FROM sales_flats sf
            WHERE sf.residential_complex = ?
        """

        cursor = self.db.conn.execute(latest_sales_query, (jk_name,))
        latest_sales = [dict(row) for row in cursor.fetchall() if row['rn'] == 1]

        if not latest_sales:
            logger.warning(f"No sales data found for JK: {jk_name}")
            return CurrentMarketAnalysis(
                jk_name=jk_name,
                global_stats=PriceStats(0, 0, 0, 0, 0),
                flat_type_buckets={},
                opportunities={},
                analysis_date=datetime.now().strftime('%Y-%m-%d')
            )

        # Calculate global statistics
        prices = [sale['price'] for sale in latest_sales]
        global_stats = self._calculate_price_stats(prices)

        # Calculate statistics by flat type
        flat_type_buckets = {}
        for flat_type in ['Studio', '1BR', '2BR', '3BR+']:
            type_sales = [sale for sale in latest_sales if sale['flat_type'] == flat_type]
            if type_sales:
                type_prices = [sale['price'] for sale in type_sales]
                flat_type_buckets[flat_type] = self._calculate_price_stats(type_prices)

        # Find opportunities (flats selling below market average by discount_percentage)
        opportunities = self._find_opportunities(latest_sales, flat_type_buckets, discount_percentage, jk_name)

        return CurrentMarketAnalysis(
            jk_name=jk_name,
            global_stats=global_stats,
            flat_type_buckets=flat_type_buckets,
            opportunities=opportunities,
            analysis_date=datetime.now().strftime('%Y-%m-%d')
        )

    def _analyze_historical_trends(self, jk_name: str, days_back: int = 365) -> HistoricalAnalysis:
        """
        Analyze historical trends for a JK.
        
        :param jk_name: str, name of the residential complex
        :param days_back: int, number of days to look back (default: 365)
        :return: HistoricalAnalysis, historical analysis
        """
        logger.info(f"Analyzing historical trends for {jk_name}")

        self.db.connect()

        # Get historical data
        start_date = (datetime.now() - timedelta(days=days_back)).strftime('%Y-%m-%d')

        historical_query = """
            SELECT query_date, flat_type, price, area, floor, total_floors, flat_id, url
            FROM sales_flats
            WHERE residential_complex = ? AND query_date >= ?
            ORDER BY query_date, flat_type
        """

        cursor = self.db.conn.execute(historical_query, (jk_name, start_date))
        historical_data = [dict(row) for row in cursor.fetchall()]

        # Group by flat type and create time series
        flat_type_timeseries = {}

        for flat_type in ['Studio', '1BR', '2BR', '3BR+']:
            type_data = [d for d in historical_data if d['flat_type'] == flat_type]

            if type_data:
                # Group by date and calculate daily statistics
                daily_stats = {}
                for record in type_data:
                    date = record['query_date']
                    if date not in daily_stats:
                        daily_stats[date] = []
                    daily_stats[date].append(record['price'])

                # Create time series data points
                timeseries = []
                for date, prices in sorted(daily_stats.items()):
                    if prices:
                        stats = self._calculate_price_stats(prices)
                        timeseries.append(StatsForFlatType(
                            date=date,
                            flat_type=flat_type,
                            residential_complex=jk_name,
                            mean_price=stats.mean,
                            median_price=stats.median,
                            min_price=stats.min,
                            max_price=stats.max,
                            count=stats.count
                        ))

                flat_type_timeseries[flat_type] = timeseries

        return HistoricalAnalysis(
            jk_name=jk_name,
            flat_type_timeseries=flat_type_timeseries,
            analysis_period=(start_date, datetime.now().strftime('%Y-%m-%d'))
        )

    def _calculate_price_stats(self, prices: List[int]) -> PriceStats:
        """
        Calculate price statistics for a list of prices.
        
        :param prices: List[int], list of prices
        :return: PriceStats, calculated statistics
        """
        if not prices:
            return PriceStats(0, 0, 0, 0, 0)

        prices_sorted = sorted(prices)
        n = len(prices_sorted)

        mean = sum(prices) / n
        median = prices_sorted[n // 2] if n % 2 == 1 else (prices_sorted[n // 2 - 1] + prices_sorted[n // 2]) / 2
        min_price = min(prices)
        max_price = max(prices)

        return PriceStats(
            mean=round(mean, 2),
            median=round(median, 2),
            min=min_price,
            max=max_price,
            count=n
        )

    def _find_opportunities(self, sales_data: List[Dict], flat_type_buckets: Dict[str, PriceStats], 
                           discount_percentage: float, jk_name: str) -> Dict[str, List[FlatOpportunity]]:
        """
        Find good opportunity flats based on discount percentage.
        
        :param sales_data: List[Dict], current sales data
        :param flat_type_buckets: Dict[str, PriceStats], price statistics by flat type
        :param discount_percentage: float, discount percentage threshold
        :return: Dict[str, List[FlatOpportunity]], opportunities grouped by flat type
        """
        opportunities_by_type = {}
        
        for sale in sales_data:
            flat_type = sale['flat_type']
            if flat_type in flat_type_buckets:
                market_avg = flat_type_buckets[flat_type].mean
                market_median = flat_type_buckets[flat_type].median
                threshold_price = market_avg * (1 - discount_percentage)
                
                if sale['price'] <= threshold_price:
                    # Calculate discount vs median (more meaningful than vs mean)
                    discount_vs_median = ((market_median - sale['price']) / market_median) * 100
                    
                    # Create FlatInfo object
                    flat_info = FlatInfo(
                        flat_id=sale['flat_id'],
                        price=sale['price'],
                        area=sale['area'],
                        flat_type=flat_type,
                        residential_complex=sale.get('residential_complex'),
                        floor=sale['floor'],
                        total_floors=sale['total_floors'],
                        construction_year=sale.get('construction_year'),
                        parking=sale.get('parking'),
                        description=sale.get('description', ''),
                        is_rental=False  # This is sales data
                    )
                    
                    # Create StatsForFlatType object
                    stats_for_flat_type = StatsForFlatType(
                        date=sale['query_date'],
                        flat_type=flat_type,
                        residential_complex=jk_name,
                        mean_price=market_avg,
                        median_price=market_median,
                        min_price=flat_type_buckets[flat_type].min,
                        max_price=flat_type_buckets[flat_type].max,
                        count=flat_type_buckets[flat_type].count
                    )
                    
                    opportunity = FlatOpportunity(
                        flat_info=flat_info,
                        stats_for_flat_type=stats_for_flat_type,
                        discount_percentage_vs_median=round(discount_vs_median, 2),
                        query_date=sale['query_date']
                    )
                    
                    # Group by flat type
                    if flat_type not in opportunities_by_type:
                        opportunities_by_type[flat_type] = []
                    opportunities_by_type[flat_type].append(opportunity)
        
        # Sort each flat type's opportunities by discount percentage vs median (highest first)
        for flat_type in opportunities_by_type:
            opportunities_by_type[flat_type].sort(key=lambda x: x.discount_percentage_vs_median, reverse=True)
        
        return opportunities_by_type

    def get_jk_list(self) -> List[str]:
        """
        Get list of all JKs in the database.
        
        :return: List[str], list of JK names
        """
        self.db.connect()

        query = """
            SELECT DISTINCT residential_complex
            FROM sales_flats
            WHERE residential_complex IS NOT NULL
            ORDER BY residential_complex
        """

        cursor = self.db.conn.execute(query)
        return [row[0] for row in cursor.fetchall()]

    def get_jk_sales_summary(self, jk_name: str) -> Dict:
        """
        Get a quick summary of sales data for a JK.
        
        :param jk_name: str, name of the residential complex
        :return: Dict, summary statistics
        """
        self.db.connect()

        # Get total sales count
        count_query = "SELECT COUNT(*) FROM sales_flats WHERE residential_complex = ?"
        total_sales = self.db.conn.execute(count_query, (jk_name,)).fetchone()[0]

        # Get date range
        date_query = """
            SELECT MIN(query_date) as earliest, MAX(query_date) as latest
            FROM sales_flats
            WHERE residential_complex = ?
        """
        date_result = self.db.conn.execute(date_query, (jk_name,)).fetchone()

        # Get flat type distribution
        type_query = """
            SELECT flat_type, COUNT(*) as count
            FROM sales_flats
            WHERE residential_complex = ?
            GROUP BY flat_type
            ORDER BY count DESC
        """
        type_distribution = dict(self.db.conn.execute(type_query, (jk_name,)).fetchall())

        return {
            "jk_name": jk_name,
            "total_sales": total_sales,
            "date_range": {
                "earliest": date_result[0],
                "latest": date_result[1]
            },
            "flat_type_distribution": type_distribution
        }


def analyze_jk_for_sales(jk_name: str, sale_discount_percentage: float = 0.15, db_path: str = "flats.db") -> Dict:
    """
    Convenience function to analyze JK sales data.
    
    :param jk_name: str, name of the residential complex
    :param sale_discount_percentage: float, discount percentage for opportunities
    :param db_path: str, path to database file
    :return: Dict, comprehensive analysis
    """
    analytics = JKAnalytics(db_path)
    return analytics.analyse_jk_for_sales(jk_name, sale_discount_percentage)