"""
JK (Residential Complex) Rentals Analytics Module.

This module provides comprehensive analytics for residential complex rental data,
including rental yield analysis, market trends, and opportunity identification.
"""

import logging
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timedelta
from dataclasses import dataclass

from db.src.write_read_database import OrthancDB
from common.src.flat_info import FlatInfo

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class PriceStats:
    """Price statistics for a group of flats."""
    mean: float
    median: float
    min: int
    max: int
    count: int

@dataclass
class RentalYieldStats:
    """Rental yield statistics for a group of flats."""
    mean_yield: float
    median_yield: float
    min_yield: float
    max_yield: float
    count: int

@dataclass
class StatsForFlatType:
    """
    Historical data point for time series analysis.
    """
    date: str
    flat_type: str
    residential_complex: str
    mean_rental: float
    median_rental: float
    min_rental: float
    max_rental: float
    mean_yield: float
    median_yield: float
    count: int

@dataclass
class RentalOpportunity:
    """
    Represents a good rental opportunity.
    """
    flat_info: FlatInfo
    stats_for_flat_type: StatsForFlatType
    yield_percentage: float
    query_date: str

@dataclass
class CurrentRentalMarketAnalysis:
    """
    Current rental market analysis for a JK.
    """
    jk_name: str
    global_stats: RentalYieldStats
    flat_type_buckets: Dict[str, RentalYieldStats]
    opportunities: Dict[str, List[RentalOpportunity]]
    analysis_date: str

@dataclass
class HistoricalRentalAnalysis:
    """
    Historical rental analysis for a JK.
    """
    jk_name: str
    flat_type_timeseries: Dict[str, List[StatsForFlatType]]
    analysis_period: Tuple[str, str]  # (start_date, end_date)

class JKRentalAnalytics:
    """
    Analytics engine for residential complex (JK) rental data.
    """
    
    def __init__(self, db_path: str = "flats.db"):
        """
        Initialize JK rental analytics.
        
        :param db_path: str, path to database file
        """
        self.db = OrthancDB(db_path)
    
    def analyse_jk_for_rentals(self, jk_name: str, min_yield_percentage: float = 0.05) -> Dict:
        """
        Analyze rental data for a specific residential complex (JK).
        
        :param jk_name: str, name of the residential complex
        :param min_yield_percentage: float, minimum yield percentage to identify opportunities (default: 5%)
        :return: Dict, comprehensive analysis including current market and historical data
        """
        logger.info(f"Starting rental analysis for JK: {jk_name}")

        try:
            # Get current market analysis
            current_analysis = self._analyze_current_rental_market(jk_name, min_yield_percentage)

            # Get historical analysis
            historical_analysis = self._analyze_historical_rental_trends(jk_name)

            return {
                "jk_name": jk_name,
                "analysis_timestamp": datetime.now().isoformat(),
                "min_yield_percentage": min_yield_percentage,
                "current_market": current_analysis,
                "historical_analysis": historical_analysis
            }

        except Exception as e:
            logger.error(f"Error analyzing JK {jk_name}: {e}")
            raise

    def _analyze_current_rental_market(self, jk_name: str, min_yield_percentage: float) -> CurrentRentalMarketAnalysis:
        """
        Analyze current rental market conditions for a JK.
        
        :param jk_name: str, name of the residential complex
        :param min_yield_percentage: float, minimum yield percentage for opportunities
        :return: CurrentRentalMarketAnalysis, current market analysis
        """
        logger.info(f"Analyzing current rental market for {jk_name}")

        self.db.connect()

        # Get latest rental data (most recent query_date for each flat)
        latest_rentals_query = """
            SELECT rf.*, 
                   ROW_NUMBER() OVER (PARTITION BY rf.flat_id ORDER BY rf.query_date DESC) as rn
            FROM rental_flats rf
            WHERE rf.residential_complex = ?
        """

        cursor = self.db.conn.execute(latest_rentals_query, (jk_name,))
        latest_rentals = [dict(row) for row in cursor.fetchall() if row['rn'] == 1]

        if not latest_rentals:
            logger.warning(f"No rental data found for JK: {jk_name}")
            return CurrentRentalMarketAnalysis(
                jk_name=jk_name,
                global_stats=RentalYieldStats(0, 0, 0, 0, 0),
                flat_type_buckets={},
                opportunities={},
                analysis_date=datetime.now().strftime('%Y-%m-%d')
            )

        # Calculate rental yields (annual rental / sales price * 100)
        rental_yields = []
        for rental in latest_rentals:
            # Get corresponding sales price for the same flat
            sales_price = self._get_sales_price_for_flat(rental['flat_id'])
            if sales_price and sales_price > 0:
                annual_rental = rental['price'] * 12  # Monthly to annual
                yield_pct = (annual_rental / sales_price) * 100
                rental_yields.append(yield_pct)
                rental['yield_percentage'] = yield_pct
                rental['sales_price'] = sales_price
            else:
                rental['yield_percentage'] = None
                rental['sales_price'] = None

        # Calculate global statistics
        valid_yields = [y for y in rental_yields if y is not None]
        if not valid_yields:
            logger.warning(f"No valid rental yields found for JK: {jk_name}")
            return CurrentRentalMarketAnalysis(
                jk_name=jk_name,
                global_stats=RentalYieldStats(0, 0, 0, 0, 0),
                flat_type_buckets={},
                opportunities={},
                analysis_date=datetime.now().strftime('%Y-%m-%d')
            )

        global_stats = self._calculate_yield_stats(valid_yields)

        # Calculate statistics by flat type
        flat_type_buckets = {}
        for flat_type in ['Studio', '1BR', '2BR', '3BR+']:
            type_rentals = [r for r in latest_rentals if r['flat_type'] == flat_type and r.get('yield_percentage') is not None]
            if type_rentals:
                type_yields = [r['yield_percentage'] for r in type_rentals]
                flat_type_buckets[flat_type] = self._calculate_yield_stats(type_yields)

        # Find opportunities (rentals with yield >= min_yield_percentage)
        opportunities = self._find_rental_opportunities(latest_rentals, flat_type_buckets, min_yield_percentage, jk_name)

        self.db.disconnect()

        return CurrentRentalMarketAnalysis(
            jk_name=jk_name,
            global_stats=global_stats,
            flat_type_buckets=flat_type_buckets,
            opportunities=opportunities,
            analysis_date=datetime.now().strftime('%Y-%m-%d')
        )

    def _get_sales_price_for_flat(self, flat_id: str) -> Optional[float]:
        """
        Get the most recent sales price for a flat.
        
        :param flat_id: str, flat ID
        :return: Optional[float], sales price or None if not found
        """
        try:
            cursor = self.db.conn.execute("""
                SELECT price FROM sales_flats 
                WHERE flat_id = ? 
                ORDER BY query_date DESC 
                LIMIT 1
            """, (flat_id,))
            result = cursor.fetchone()
            return result[0] if result else None
        except Exception as e:
            logger.warning(f"Error getting sales price for flat {flat_id}: {e}")
            return None

    def _calculate_yield_stats(self, yields: List[float]) -> RentalYieldStats:
        """
        Calculate yield statistics from a list of yields.
        
        :param yields: List[float], list of yield percentages
        :return: RentalYieldStats, calculated statistics
        """
        if not yields:
            return RentalYieldStats(0, 0, 0, 0, 0)
        
        yields_sorted = sorted(yields)
        n = len(yields_sorted)
        
        mean = sum(yields) / n
        median = yields_sorted[n // 2] if n % 2 == 1 else (yields_sorted[n // 2 - 1] + yields_sorted[n // 2]) / 2
        min_yield = min(yields)
        max_yield = max(yields)
        
        return RentalYieldStats(mean, median, min_yield, max_yield, n)

    def _find_rental_opportunities(self, rentals_data: List[Dict], flat_type_buckets: Dict[str, RentalYieldStats], 
                                  min_yield_percentage: float, jk_name: str) -> Dict[str, List[RentalOpportunity]]:
        """
        Find good rental opportunities based on yield percentage.
        
        :param rentals_data: List[Dict], current rental data
        :param flat_type_buckets: Dict[str, RentalYieldStats], yield statistics by flat type
        :param min_yield_percentage: float, minimum yield percentage threshold
        :param jk_name: str, name of the residential complex
        :return: Dict[str, List[RentalOpportunity]], opportunities grouped by flat type
        """
        opportunities_by_type = {}
        
        for rental in rentals_data:
            if rental.get('yield_percentage') is None:
                continue
                
            flat_type = rental['flat_type']
            if flat_type in flat_type_buckets:
                market_avg_yield = flat_type_buckets[flat_type].mean_yield
                market_median_yield = flat_type_buckets[flat_type].median_yield
                
                if rental['yield_percentage'] >= min_yield_percentage:
                    # Create FlatInfo object
                    flat_info = FlatInfo(
                        flat_id=rental['flat_id'],
                        price=rental['price'],
                        area=rental['area'],
                        flat_type=flat_type,
                        residential_complex=rental.get('residential_complex'),
                        floor=rental['floor'],
                        total_floors=rental['total_floors'],
                        construction_year=rental.get('construction_year'),
                        parking=rental.get('parking'),
                        description=rental.get('description', ''),
                        is_rental=True
                    )
                    
                    stats_for_flat_type = StatsForFlatType(
                        date=rental['query_date'],
                        flat_type=flat_type,
                        residential_complex=jk_name,
                        mean_rental=market_avg_yield,
                        median_rental=market_median_yield,
                        min_rental=flat_type_buckets[flat_type].min_yield,
                        max_rental=flat_type_buckets[flat_type].max_yield,
                        mean_yield=market_avg_yield,
                        median_yield=market_median_yield,
                        count=flat_type_buckets[flat_type].count
                    )
                    
                    opportunity = RentalOpportunity(
                        flat_info=flat_info,
                        stats_for_flat_type=stats_for_flat_type,
                        yield_percentage=rental['yield_percentage'],
                        query_date=rental['query_date']
                    )
                    
                    if flat_type not in opportunities_by_type:
                        opportunities_by_type[flat_type] = []
                    opportunities_by_type[flat_type].append(opportunity)
        
        # Sort opportunities by yield percentage (highest first)
        for flat_type in opportunities_by_type:
            opportunities_by_type[flat_type].sort(key=lambda x: x.yield_percentage, reverse=True)
        
        return opportunities_by_type

    def _analyze_historical_rental_trends(self, jk_name: str) -> HistoricalRentalAnalysis:
        """
        Analyze historical rental trends for a JK.
        
        :param jk_name: str, name of the residential complex
        :return: HistoricalRentalAnalysis, historical analysis
        """
        logger.info(f"Analyzing historical rental trends for {jk_name}")

        self.db.connect()

        # Get historical rental data grouped by date and flat type
        historical_query = """
            SELECT 
                DATE(rf.query_date) as date,
                rf.flat_type,
                COUNT(*) as count,
                AVG(rf.price) as mean_rental,
                MIN(rf.price) as min_rental,
                MAX(rf.price) as max_rental,
                rf.residential_complex
            FROM rental_flats rf
            WHERE rf.residential_complex = ?
            GROUP BY DATE(rf.query_date), rf.flat_type
            ORDER BY date DESC, rf.flat_type
        """

        cursor = self.db.conn.execute(historical_query, (jk_name,))
        historical_data = [dict(row) for row in cursor.fetchall()]

        # Group by flat type
        flat_type_timeseries = {}
        for row in historical_data:
            flat_type = row['flat_type']
            if flat_type not in flat_type_timeseries:
                flat_type_timeseries[flat_type] = []
            
            # Calculate median (approximate)
            median_rental = (row['min_rental'] + row['max_rental']) / 2
            
            # Calculate yield (simplified - would need sales data for accurate calculation)
            mean_yield = 0.0  # Placeholder - would need sales price data
            median_yield = 0.0  # Placeholder
            
            stats_point = StatsForFlatType(
                date=row['date'],
                flat_type=flat_type,
                residential_complex=row['residential_complex'],
                mean_rental=row['mean_rental'],
                median_rental=median_rental,
                min_rental=row['min_rental'],
                max_rental=row['max_rental'],
                mean_yield=mean_yield,
                median_yield=median_yield,
                count=row['count']
            )
            
            flat_type_timeseries[flat_type].append(stats_point)

        # Determine analysis period
        if historical_data:
            dates = [row['date'] for row in historical_data]
            start_date = min(dates)
            end_date = max(dates)
        else:
            start_date = end_date = datetime.now().strftime('%Y-%m-%d')

        self.db.disconnect()

        return HistoricalRentalAnalysis(
            jk_name=jk_name,
            flat_type_timeseries=flat_type_timeseries,
            analysis_period=(start_date, end_date)
        )

    def get_jk_list(self) -> List[str]:
        """
        Get list of all JKs with rental data.
        
        :return: List[str], list of JK names
        """
        self.db.connect()
        cursor = self.db.conn.execute("""
            SELECT DISTINCT residential_complex 
            FROM rental_flats 
            WHERE residential_complex IS NOT NULL
            ORDER BY residential_complex
        """)
        jks = [row[0] for row in cursor.fetchall()]
        self.db.disconnect()
        return jks

    def get_jk_rentals_summary(self, jk_name: str) -> Dict:
        """
        Get rental summary for a specific JK.
        
        :param jk_name: str, name of the residential complex
        :return: Dict, rental summary
        """
        self.db.connect()
        
        # Get rental statistics
        cursor = self.db.conn.execute("""
            SELECT 
                COUNT(*) as total_rentals,
                MIN(query_date) as earliest,
                MAX(query_date) as latest
            FROM rental_flats 
            WHERE residential_complex = ?
        """, (jk_name,))
        
        stats = dict(cursor.fetchone())
        
        # Get flat type distribution
        cursor = self.db.conn.execute("""
            SELECT 
                flat_type,
                COUNT(*) as count
            FROM rental_flats 
            WHERE residential_complex = ?
            GROUP BY flat_type
            ORDER BY count DESC
        """, (jk_name,))
        
        flat_type_distribution = {row[0]: row[1] for row in cursor.fetchall()}
        
        self.db.disconnect()
        
        return {
            "jk_name": jk_name,
            "total_rentals": stats['total_rentals'],
            "date_range": {
                "earliest": stats['earliest'],
                "latest": stats['latest']
            },
            "flat_type_distribution": flat_type_distribution
        }


def analyze_jk_for_rentals(jk_name: str, min_yield_percentage: float = 0.05, db_path: str = "flats.db") -> Dict:
    """
    Convenience function to analyze JK rental data.
    
    :param jk_name: str, name of the residential complex
    :param min_yield_percentage: float, minimum yield percentage for opportunities
    :param db_path: str, path to database file
    :return: Dict, comprehensive analysis
    """
    analytics = JKRentalAnalytics(db_path)
    return analytics.analyse_jk_for_rentals(jk_name, min_yield_percentage)


