"""
Data classes for API analysis responses.
"""
from dataclasses import dataclass
from typing import Dict, List, Optional
from common.src.flat_info import FlatInfo


@dataclass
class GlobalStats:
    """Global statistics for analysis."""
    mean: float
    median: float
    min: float
    max: float
    count: int


@dataclass
class RentalGlobalStats:
    """Global statistics for rental analysis."""
    mean_yield: float
    median_yield: float
    min_yield: float
    max_yield: float
    count: int


@dataclass
class FlatTypeStats:
    """Statistics for a specific flat type."""
    mean: float
    median: float
    min: float
    max: float
    count: int


@dataclass
class RentalFlatTypeStats:
    """Rental statistics for a specific flat type."""
    mean_yield: float
    median_yield: float
    min_yield: float
    max_yield: float
    count: int


@dataclass
class Opportunity:
    """Represents a good opportunity flat."""
    flat_id: str
    price: float
    area: float
    flat_type: str
    residential_complex: str
    floor: int
    total_floors: int
    construction_year: Optional[int]
    parking: bool
    description: str
    is_rental: bool
    discount_percentage_vs_median: Optional[float] = None
    yield_percentage: Optional[float] = None
    market_stats: Optional[Dict] = None
    query_date: Optional[str] = None


@dataclass
class CurrentMarket:
    """Current market analysis."""
    global_stats: GlobalStats
    flat_type_buckets: Dict[str, FlatTypeStats]
    opportunities: Dict[str, List[Opportunity]]


@dataclass
class RentalCurrentMarket:
    """Current rental market analysis."""
    global_stats: RentalGlobalStats
    flat_type_buckets: Dict[str, RentalFlatTypeStats]
    opportunities: Dict[str, List[Opportunity]]


@dataclass
class HistoricalPoint:
    """Historical data point for time series analysis."""
    date: str
    flat_type: str
    residential_complex: str
    mean_price: float
    median_price: float
    min_price: float
    max_price: float
    count: int


@dataclass
class RentalHistoricalPoint:
    """Historical rental data point for time series analysis."""
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
class HistoricalAnalysis:
    """Historical analysis data."""
    flat_type_timeseries: Dict[str, List[HistoricalPoint]]


@dataclass
class RentalHistoricalAnalysis:
    """Historical rental analysis data."""
    flat_type_timeseries: Dict[str, List[RentalHistoricalPoint]]


@dataclass
class SalesAnalysisResponse:
    """Sales analysis API response."""
    success: bool
    jk_name: str
    discount_percentage: float
    current_market: CurrentMarket
    historical_analysis: HistoricalAnalysis
    error: Optional[str] = None


@dataclass
class RentalAnalysisResponse:
    """Rental analysis API response."""
    success: bool
    jk_name: str
    min_yield_percentage: float
    current_market: RentalCurrentMarket
    historical_analysis: RentalHistoricalAnalysis
    error: Optional[str] = None


@dataclass
class FlatTypeAnalysisResponse:
    """Flat type analysis API response."""
    success: bool
    residential_complex_name: str
    query_date: str
    area_max: float
    analysis_by_flat_type: Dict[str, Dict]
    total_rental_flats: int
    total_sales_flats: int
    overall_stats: Dict
    error: Optional[str] = None
