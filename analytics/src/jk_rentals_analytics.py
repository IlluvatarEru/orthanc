"""
JK (Residential Complex) Rentals Analytics Module.

This module provides comprehensive analytics for residential complex rental data,
including rental yield analysis, market trends, and opportunity identification.
"""

import logging
from typing import Dict, List, Tuple
from datetime import datetime, timedelta
from dataclasses import dataclass

from common.src.flat_type import FlatType, FLAT_TYPE_VALUES
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
    min_yield: float
    max_yield: float
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

    def analyse_jk_for_rentals(
        self, jk_name: str, min_yield_percentage: float = 0.05
    ) -> Dict:
        """
        Analyze rental data for a specific residential complex (JK).

        :param jk_name: str, name of the residential complex
        :param min_yield_percentage: float, minimum yield percentage to identify opportunities (default: 5%)
        :return: Dict, comprehensive analysis including current market and historical data
        """
        logger.info(f"Starting rental analysis for JK: {jk_name}")

        try:
            # Get current market analysis
            current_analysis = self._analyze_current_rental_market(
                jk_name, min_yield_percentage
            )

            # Get historical analysis
            historical_analysis = self._analyze_historical_rental_trends(jk_name)

            return {
                "jk_name": jk_name,
                "analysis_timestamp": datetime.now().isoformat(),
                "min_yield_percentage": min_yield_percentage,
                "current_market": current_analysis,
                "historical_analysis": historical_analysis,
            }

        except Exception as e:
            logger.error(f"Error analyzing JK {jk_name}: {e}")
            raise

    def _analyze_current_rental_market(
        self, jk_name: str, min_yield_percentage: float
    ) -> CurrentRentalMarketAnalysis:
        """
        Analyze current rental market conditions for a JK.

        :param jk_name: str, name of the residential complex
        :param min_yield_percentage: float, minimum yield percentage for opportunities
        :return: CurrentRentalMarketAnalysis, current market analysis
        """
        logger.info(f"Analyzing current rental market for {jk_name}")

        # Get latest rental data (most recent query_date for each flat)
        latest_rentals = self.db.get_latest_rentals_for_jk(jk_name)

        if not latest_rentals:
            logger.warning(f"No rental data found for JK: {jk_name}")
            return CurrentRentalMarketAnalysis(
                jk_name=jk_name,
                global_stats=RentalYieldStats(0, 0, 0, 0, 0),
                flat_type_buckets={},
                opportunities={},
                analysis_date=datetime.now().strftime("%Y-%m-%d"),
            )

        # Get all sales flats for this JK to find similar ones for yield calculation
        latest_sales = self.db.get_latest_sales_for_jk(jk_name)

        # Calculate rental yields (annual rental / sales price * 100)
        # Use median price of similar sales flats (same flat_type, area within 20%)
        rental_yields = []
        for rental in latest_rentals:
            # Find similar sales flats (same flat_type, area within 20% tolerance)
            similar_sales = self._find_similar_sales_flats(
                rental, latest_sales, area_tolerance=0.20
            )

            if similar_sales and len(similar_sales) > 0:
                # Use median price of similar sales flats
                sales_prices = [s["price"] for s in similar_sales]
                sales_prices_sorted = sorted(sales_prices)
                n = len(sales_prices_sorted)
                median_sales_price = (
                    sales_prices_sorted[n // 2]
                    if n % 2 == 1
                    else (sales_prices_sorted[n // 2 - 1] + sales_prices_sorted[n // 2])
                    / 2
                )

                if median_sales_price > 0:
                    annual_rental = rental["price"] * 12
                    yield_pct = annual_rental / median_sales_price
                    rental_yields.append(yield_pct)
                    rental["yield_percentage"] = yield_pct
                    rental["sales_price"] = median_sales_price
                else:
                    rental["yield_percentage"] = None
                    rental["sales_price"] = None
            else:
                rental["yield_percentage"] = None
                rental["sales_price"] = None

        # Calculate global statistics
        valid_yields = [y for y in rental_yields if y is not None]
        if not valid_yields:
            logger.warning(f"No valid rental yields found for JK: {jk_name}")
            return CurrentRentalMarketAnalysis(
                jk_name=jk_name,
                global_stats=RentalYieldStats(0, 0, 0, 0, 0),
                flat_type_buckets={},
                opportunities={},
                analysis_date=datetime.now().strftime("%Y-%m-%d"),
            )

        global_stats = self._calculate_yield_stats(valid_yields)

        # Calculate statistics by flat type
        flat_type_buckets = {}
        for flat_type in FLAT_TYPE_VALUES:
            type_rentals = [
                r
                for r in latest_rentals
                if r["flat_type"] == flat_type and r.get("yield_percentage") is not None
            ]
            if type_rentals:
                type_yields = [r["yield_percentage"] for r in type_rentals]
                flat_type_buckets[flat_type] = self._calculate_yield_stats(type_yields)

        # Find opportunities (rentals with yield >= min_yield_percentage)
        opportunities = self._find_rental_opportunities(
            latest_rentals, flat_type_buckets, min_yield_percentage, jk_name
        )

        return CurrentRentalMarketAnalysis(
            jk_name=jk_name,
            global_stats=global_stats,
            flat_type_buckets=flat_type_buckets,
            opportunities=opportunities,
            analysis_date=datetime.now().strftime("%Y-%m-%d"),
        )

    def _find_similar_sales_flats(
        self, rental: Dict, sales_flats: List[Dict], area_tolerance: float = 0.20
    ) -> List[Dict]:
        """
        Find similar sales flats for a rental flat based on area and flat type.

        :param rental: Dict, rental flat data
        :param sales_flats: List[Dict], list of sales flats for the same JK
        :param area_tolerance: float, area tolerance (0.20 = 20%)
        :return: List[Dict], list of similar sales flats
        """
        similar = []
        rental_area = rental.get("area")
        rental_flat_type = rental.get("flat_type")

        if rental_area is None or rental_flat_type is None:
            logger.warning(
                f"Rental {rental.get('flat_id')} missing area or flat_type: area={rental_area}, flat_type={rental_flat_type}"
            )
            return similar

        for sale in sales_flats:
            sale_flat_type = sale.get("flat_type")
            sale_area = sale.get("area")

            if sale_flat_type is None or sale_area is None:
                continue

            # Match by flat_type and area within tolerance (Studio and 1BR mixed because kazakhs are bad at distinguishing them)
            if (
                (sale_flat_type == rental_flat_type)
                or (
                    rental_flat_type == FlatType.STUDIO.value
                    and sale_flat_type == FlatType.ONE_BEDROOM.value
                )
                or (
                    rental_flat_type == FlatType.ONE_BEDROOM.value
                    and sale_flat_type == FlatType.STUDIO.value
                )
            ):
                # Check if areas are within tolerance (20% of each other)
                if max(sale_area, rental_area) > 0:
                    area_diff = abs(sale_area - rental_area) / max(
                        sale_area, rental_area
                    )
                    if area_diff <= area_tolerance:
                        similar.append(sale)

        return similar

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
        median = (
            yields_sorted[n // 2]
            if n % 2 == 1
            else (yields_sorted[n // 2 - 1] + yields_sorted[n // 2]) / 2
        )
        min_yield = min(yields)
        max_yield = max(yields)

        return RentalYieldStats(mean, median, min_yield, max_yield, n)

    def _find_rental_opportunities(
        self,
        rentals_data: List[Dict],
        flat_type_buckets: Dict[str, RentalYieldStats],
        min_yield_percentage: float,
        jk_name: str,
    ) -> Dict[str, List[RentalOpportunity]]:
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
            if rental.get("yield_percentage") is None:
                continue

            flat_type = rental["flat_type"]
            if flat_type in flat_type_buckets:
                bucket_for_flat_type = flat_type_buckets[flat_type]
                market_avg_yield = bucket_for_flat_type.mean_yield
                market_median_yield = bucket_for_flat_type.median_yield
                market_min_yield = bucket_for_flat_type.min_yield
                market_max_yield = bucket_for_flat_type.max_yield

                if rental["yield_percentage"] >= min_yield_percentage:
                    # Create FlatInfo object
                    flat_info = FlatInfo(
                        flat_id=rental["flat_id"],
                        price=rental["price"],
                        area=rental["area"],
                        flat_type=flat_type,
                        residential_complex=rental.get("residential_complex"),
                        floor=rental["floor"],
                        total_floors=rental["total_floors"],
                        construction_year=rental.get("construction_year"),
                        parking=rental.get("parking"),
                        description=rental.get("description", ""),
                        is_rental=True,
                    )

                    stats_for_flat_type = StatsForFlatType(
                        date=rental["query_date"],
                        flat_type=flat_type,
                        residential_complex=jk_name,
                        mean_rental=market_avg_yield,
                        median_rental=market_median_yield,
                        min_rental=bucket_for_flat_type.min_yield,
                        max_rental=bucket_for_flat_type.max_yield,
                        mean_yield=market_avg_yield,
                        median_yield=market_median_yield,
                        min_yield=market_min_yield,
                        max_yield=market_max_yield,
                        count=bucket_for_flat_type.count,
                    )

                    opportunity = RentalOpportunity(
                        flat_info=flat_info,
                        stats_for_flat_type=stats_for_flat_type,
                        yield_percentage=rental["yield_percentage"],
                        query_date=rental["query_date"],
                    )

                    if flat_type not in opportunities_by_type:
                        opportunities_by_type[flat_type] = []
                    opportunities_by_type[flat_type].append(opportunity)

        # Sort opportunities by yield percentage (highest first)
        for flat_type in opportunities_by_type:
            opportunities_by_type[flat_type].sort(
                key=lambda x: x.yield_percentage, reverse=True
            )

        return opportunities_by_type

    def _analyze_historical_rental_trends(
        self, jk_name: str
    ) -> HistoricalRentalAnalysis:
        """
        Analyze historical rental trends for a JK.

        :param jk_name: str, name of the residential complex
        :return: HistoricalRentalAnalysis, historical analysis
        """
        logger.info(f"Analyzing historical rental trends for {jk_name}")

        # Get historical rental data grouped by date and flat type
        historical_data = self.db.get_historical_rental_stats_by_jk(jk_name)

        # Determine analysis period
        if historical_data:
            dates = [row["date"] for row in historical_data]
            start_date = min(dates)
            end_date = max(dates)
        else:
            start_date = end_date = datetime.now().strftime("%Y-%m-%d")

        # Group by flat type and calculate yields for each date
        flat_type_timeseries = {}
        for row in historical_data:
            flat_type = row["flat_type"]
            date = row["date"]

            if flat_type not in flat_type_timeseries:
                flat_type_timeseries[flat_type] = []

            # Get actual rental flats for this date and flat type
            rentals = self.db.get_rentals_by_date_and_flat_type(
                jk_name, date, flat_type
            )

            # Get sales flats from a date range around this date (±7 days)
            date_obj = datetime.strptime(date, "%Y-%m-%d")
            date_start = (date_obj - timedelta(days=7)).strftime("%Y-%m-%d")
            date_end = (date_obj + timedelta(days=7)).strftime("%Y-%m-%d")
            sales = self.db.get_sales_by_date_range(jk_name, date_start, date_end)

            # Calculate yields for each rental (same logic as current market analysis)
            rental_yields = []
            for rental in rentals:
                # Find similar sales flats (same flat_type, area within 20% tolerance)
                similar_sales = self._find_similar_sales_flats(
                    rental, sales, area_tolerance=0.20
                )

                if similar_sales and len(similar_sales) > 0:
                    # Use median price of similar sales flats
                    sales_prices = [s["price"] for s in similar_sales]
                    sales_prices_sorted = sorted(sales_prices)
                    n = len(sales_prices_sorted)
                    median_sales_price = (
                        sales_prices_sorted[n // 2]
                        if n % 2 == 1
                        else (
                            sales_prices_sorted[n // 2 - 1]
                            + sales_prices_sorted[n // 2]
                        )
                        / 2
                    )

                    if median_sales_price > 0:
                        annual_rental = rental["price"] * 12
                        yield_pct = annual_rental / median_sales_price
                        rental_yields.append(yield_pct)

            # Calculate yield statistics using the same method as current market
            if rental_yields:
                yield_stats = self._calculate_yield_stats(rental_yields)
                mean_yield = yield_stats.mean_yield
                median_yield = yield_stats.median_yield
                min_yield = yield_stats.min_yield
                max_yield = yield_stats.max_yield
            else:
                # No valid yields calculated
                mean_yield = 0.0
                median_yield = 0.0
                min_yield = 0.0
                max_yield = 0.0

            # Calculate median rental from actual rental prices
            if rentals:
                rental_prices = sorted([r["price"] for r in rentals])
                n = len(rental_prices)
                median_rental = (
                    rental_prices[n // 2]
                    if n % 2 == 1
                    else (rental_prices[n // 2 - 1] + rental_prices[n // 2]) / 2
                )
            else:
                # Fallback to approximation if no rentals found
                median_rental = (row["min_rental"] + row["max_rental"]) / 2

            stats_point = StatsForFlatType(
                date=date,
                flat_type=flat_type,
                residential_complex=row["residential_complex"],
                mean_rental=row["mean_rental"],
                median_rental=median_rental,
                min_rental=row["min_rental"],
                max_rental=row["max_rental"],
                mean_yield=mean_yield,
                median_yield=median_yield,
                min_yield=min_yield,
                max_yield=max_yield,
                count=row["count"],
            )

            flat_type_timeseries[flat_type].append(stats_point)

        return HistoricalRentalAnalysis(
            jk_name=jk_name,
            flat_type_timeseries=flat_type_timeseries,
            analysis_period=(start_date, end_date),
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
        # Get rental statistics
        stats = self.db.get_jk_rentals_summary_stats(jk_name)

        # Get flat type distribution
        flat_type_distribution = self.db.get_jk_rentals_flat_type_distribution(jk_name)

        return {
            "jk_name": jk_name,
            "total_rentals": stats["total_rentals"],
            "date_range": {"earliest": stats["earliest"], "latest": stats["latest"]},
            "flat_type_distribution": flat_type_distribution,
        }


def analyze_jk_for_rentals(
    jk_name: str, min_yield_percentage: float = 0.05, db_path: str = "flats.db"
) -> Dict:
    """
    Convenience function to analyze JK rental data.

    :param jk_name: str, name of the residential complex
    :param min_yield_percentage: float, minimum yield percentage for opportunities
    :param db_path: str, path to database file
    :return: Dict, comprehensive analysis
    """
    analytics = JKRentalAnalytics(db_path)
    return analytics.analyse_jk_for_rentals(jk_name, min_yield_percentage)
