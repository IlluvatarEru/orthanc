"""
API client for webapp to communicate with the FastAPI backend.
"""

import requests
import logging
from typing import Dict, List, Optional
from api.src.analysis_objects import (
    SalesAnalysisResponse,
    RentalAnalysisResponse,
    CurrentMarket,
    GlobalStats,
    FlatTypeStats,
    Opportunity,
    HistoricalAnalysis,
    HistoricalPoint,
    RentalCurrentMarket,
    RentalGlobalStats,
    RentalFlatTypeStats,
    RentalHistoricalAnalysis,
    RentalHistoricalPoint,
)

logger = logging.getLogger(__name__)

DEFAULT_TIMEOUT = 10


class WebappAPIClient:
    """Client for communicating with the FastAPI backend."""

    def __init__(self, base_url: str = "http://localhost:8000"):
        """
        Initialize API client.

        :param base_url: str, base URL of the FastAPI server
        """
        self.base_url = base_url.rstrip("/")

    def search_complexes(self, query: str) -> Dict:
        """Search for residential complexes."""
        try:
            response = requests.get(
                f"{self.base_url}/api/complexes/search",
                params={"q": query},
                timeout=DEFAULT_TIMEOUT,
            )
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            logger.error(f"API error searching complexes: {e}")
            return {"success": False, "error": str(e), "complexes": []}

    def get_complex_info(self, residential_complex_name: str) -> Optional[Dict]:
        """Get specific complex information."""
        try:
            response = requests.get(
                f"{self.base_url}/api/complexes/{residential_complex_name}",
                timeout=DEFAULT_TIMEOUT,
            )
            response.raise_for_status()
            data = response.json()
            return data.get("complex") if data.get("success") else None
        except requests.RequestException as e:
            logger.error(
                f"API error getting complex info for {residential_complex_name}: {e}"
            )
            return None

    def get_all_complexes(self) -> List[Dict]:
        """Get all residential complexes."""
        try:
            response = requests.get(
                f"{self.base_url}/api/complexes/", timeout=DEFAULT_TIMEOUT
            )
            response.raise_for_status()
            data = response.json()
            return data.get("complexes", []) if data.get("success") else []
        except requests.RequestException as e:
            logger.error(f"API error getting all complexes: {e}")
            return []

    def scrape_complex_data(
        self,
        residential_complex_name: str,
        complex_id: Optional[str] = None,
        only_rentals: bool = False,
        only_sales: bool = False,
    ) -> Dict:
        """Scrape data for a specific complex."""
        payload = {}
        if complex_id:
            payload["complex_id"] = complex_id
        if only_rentals:
            payload["only_rentals"] = True
        if only_sales:
            payload["only_sales"] = True

        try:
            response = requests.post(
                f"{self.base_url}/api/complexes/{residential_complex_name}/scrape",
                json=payload,
                timeout=120,
            )
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            logger.error(f"API error scraping {residential_complex_name}: {e}")
            return {"success": False, "error": str(e)}

    def refresh_complex_analysis(self, residential_complex_name: str) -> Dict:
        """Refresh analysis for a complex."""
        try:
            response = requests.post(
                f"{self.base_url}/api/complexes/{residential_complex_name}/refresh",
                timeout=30,
            )
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            logger.error(f"API error refreshing {residential_complex_name}: {e}")
            return {"success": False, "error": str(e)}

    def get_flat_info(self, flat_id: str) -> Optional[Dict]:
        """Get flat information."""
        try:
            response = requests.get(
                f"{self.base_url}/api/flats/{flat_id}", timeout=DEFAULT_TIMEOUT
            )
            response.raise_for_status()
            data = response.json()
            return data.get("flat") if data.get("success") else None
        except requests.RequestException as e:
            logger.error(f"API error getting flat info for {flat_id}: {e}")
            return None

    def get_similar_flats(
        self, flat_id: str, area_tolerance: float = 10.0, min_flats: int = 3
    ) -> Dict:
        """
        Get similar flats for investment analysis.

        :param flat_id: str, ID of the flat to find similar properties for
        :param area_tolerance: float, area tolerance percentage (default: 10.0)
        :param min_flats: int, minimum number of similar flats required in each category (default: 3)
        :return: dict with success/error fields
        """
        try:
            response = requests.get(
                f"{self.base_url}/api/flats/{flat_id}/similar",
                params={"area_tolerance": area_tolerance, "min_flats": min_flats},
                timeout=DEFAULT_TIMEOUT,
            )
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            logger.error(f"API error getting similar flats for {flat_id}: {e}")
            return {
                "success": False,
                "error": str(e),
                "rental_count": 0,
                "sales_count": 0,
                "min_required": min_flats,
            }

    def get_market_context(self, flat_id: str) -> Optional[Dict]:
        """Get first_seen date, days_on_market, and JK liquidity for a flat."""
        try:
            response = requests.get(
                f"{self.base_url}/api/flats/{flat_id}/market-context",
                timeout=DEFAULT_TIMEOUT,
            )
            response.raise_for_status()
            data = response.json()
            return data if data.get("success") else None
        except requests.RequestException as e:
            logger.error(f"API error getting market context for {flat_id}: {e}")
            return None

    def get_database_stats(self) -> Dict:
        """Get database statistics."""
        try:
            response = requests.get(
                f"{self.base_url}/api/database/stats", timeout=DEFAULT_TIMEOUT
            )
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            logger.error(f"API error getting database stats: {e}")
            return {"success": False, "error": str(e)}

    def get_jk_sales_analysis(
        self, jk_name: str, discount_percentage: float = 0.15
    ) -> SalesAnalysisResponse:
        """Get JK sales analysis."""
        try:
            response = requests.get(
                f"{self.base_url}/api/jks/sales/{jk_name}/analysis",
                params={"discount_percentage": discount_percentage},
                timeout=DEFAULT_TIMEOUT,
            )
            response.raise_for_status()
            data = response.json()
        except requests.RequestException as e:
            logger.error(f"API error getting sales analysis for {jk_name}: {e}")
            return SalesAnalysisResponse(
                success=False,
                error=str(e),
                jk_name=jk_name,
                discount_percentage=discount_percentage,
                current_market=CurrentMarket(
                    global_stats=GlobalStats(
                        count=0, mean=0, median=0, min=0, max=0, std=0
                    ),
                    flat_type_buckets={},
                    opportunities={},
                ),
                historical_analysis=HistoricalAnalysis(flat_type_timeseries={}),
            )

        # Convert nested dictionaries to objects
        if data.get("current_market"):
            current_market = data["current_market"]
            if current_market.get("global_stats"):
                current_market["global_stats"] = GlobalStats(
                    **current_market["global_stats"]
                )
            if current_market.get("flat_type_buckets"):
                for flat_type, stats in current_market["flat_type_buckets"].items():
                    current_market["flat_type_buckets"][flat_type] = FlatTypeStats(
                        **stats
                    )
            if current_market.get("opportunities"):
                for flat_type, opps in current_market["opportunities"].items():
                    current_market["opportunities"][flat_type] = [
                        Opportunity(**opp) for opp in opps
                    ]
            data["current_market"] = CurrentMarket(**current_market)

        if data.get("historical_analysis"):
            hist = data["historical_analysis"]
            if hist.get("flat_type_timeseries"):
                for flat_type, points in hist["flat_type_timeseries"].items():
                    hist["flat_type_timeseries"][flat_type] = [
                        HistoricalPoint(**point) for point in points
                    ]
            data["historical_analysis"] = HistoricalAnalysis(**hist)

        return SalesAnalysisResponse(**data)

    def get_jk_rentals_analysis(
        self, jk_name: str, min_yield_percentage: float = 0.0
    ) -> RentalAnalysisResponse:
        """Get JK rentals analysis."""
        try:
            response = requests.get(
                f"{self.base_url}/api/jks/rentals/{jk_name}/analysis",
                params={"min_yield_percentage": min_yield_percentage},
                timeout=DEFAULT_TIMEOUT,
            )
            response.raise_for_status()
            data = response.json()
        except requests.RequestException as e:
            logger.error(f"API error getting rentals analysis for {jk_name}: {e}")
            return RentalAnalysisResponse(
                success=False,
                error=str(e),
                jk_name=jk_name,
                min_yield_percentage=min_yield_percentage,
                current_market=RentalCurrentMarket(
                    global_stats=RentalGlobalStats(
                        count=0,
                        median_yield=0,
                        mean_yield=0,
                        min_yield=0,
                        max_yield=0,
                    ),
                    flat_type_buckets={},
                    opportunities={},
                ),
                historical_analysis=RentalHistoricalAnalysis(flat_type_timeseries={}),
            )

        # Convert nested dictionaries to objects
        if data.get("current_market"):
            current_market = data["current_market"]
            if current_market.get("global_stats"):
                current_market["global_stats"] = RentalGlobalStats(
                    **current_market["global_stats"]
                )
            if current_market.get("flat_type_buckets"):
                for flat_type, stats in current_market["flat_type_buckets"].items():
                    current_market["flat_type_buckets"][flat_type] = (
                        RentalFlatTypeStats(**stats)
                    )
            if current_market.get("opportunities"):
                for flat_type, opps in current_market["opportunities"].items():
                    current_market["opportunities"][flat_type] = [
                        Opportunity(**opp) for opp in opps
                    ]
            data["current_market"] = RentalCurrentMarket(**current_market)

        if data.get("historical_analysis"):
            hist = data["historical_analysis"]
            if hist.get("flat_type_timeseries"):
                for flat_type, points in hist["flat_type_timeseries"].items():
                    hist["flat_type_timeseries"][flat_type] = [
                        RentalHistoricalPoint(**point) for point in points
                    ]
            data["historical_analysis"] = RentalHistoricalAnalysis(**hist)

        return RentalAnalysisResponse(**data)


# Global API client instance
api_client = WebappAPIClient()
