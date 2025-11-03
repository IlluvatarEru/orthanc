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
        response = requests.get(
            f"{self.base_url}/api/complexes/search", params={"q": query}, timeout=5
        )
        response.raise_for_status()
        return response.json()

    def get_complex_info(self, residential_complex_name: str) -> Optional[Dict]:
        """Get specific complex information."""
        response = requests.get(
            f"{self.base_url}/api/complexes/{residential_complex_name}", timeout=5
        )
        response.raise_for_status()
        data = response.json()
        return data.get("complex") if data.get("success") else None

    def get_all_complexes(self) -> List[Dict]:
        """Get all residential complexes."""
        response = requests.get(f"{self.base_url}/api/complexes/")
        response.raise_for_status()
        data = response.json()
        return data.get("complexes", []) if data.get("success") else []

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

        response = requests.post(
            f"{self.base_url}/api/complexes/{residential_complex_name}/scrape",
            json=payload,
        )
        response.raise_for_status()
        return response.json()

    def refresh_complex_analysis(self, residential_complex_name: str) -> Dict:
        """Refresh analysis for a complex."""
        response = requests.post(
            f"{self.base_url}/api/complexes/{residential_complex_name}/refresh"
        )
        response.raise_for_status()
        return response.json()

    def get_flat_info(self, flat_id: str) -> Optional[Dict]:
        """Get flat information."""
        response = requests.get(f"{self.base_url}/api/flats/{flat_id}")
        response.raise_for_status()
        data = response.json()
        return data.get("flat") if data.get("success") else None

    def get_similar_flats(
        self, flat_id: str, area_tolerance: float = 10.0, min_flats: int = 3
    ) -> Dict:
        """
        Get similar flats for investment analysis.

        :param flat_id: str, ID of the flat to find similar properties for
        :param area_tolerance: float, area tolerance percentage (default: 10.0)
        :param min_flats: int, minimum number of similar flats required in each category (default: 3)
        :return: dict, response containing:
            - On success (success=True):
                - flat_info: dict, information about the queried flat
                - similar_rentals: list, list of similar rental flat dicts (flat_id, price, area, residential_complex, floor, construction_year)
                - similar_sales: list, list of similar sales flat dicts (flat_id, price, area, residential_complex, floor, construction_year)
                - rental_count: int, number of similar rental flats found
                - sales_count: int, number of similar sales flats found
                - area_tolerance: float, area tolerance used
            - On failure (success=False):
                - error: str, error message explaining why the request failed
                - rental_count: int, number of rental flats found (may be less than min_flats)
                - sales_count: int, number of sales flats found (may be less than min_flats)
                - min_required: int, minimum required flats per category
        """
        response = requests.get(
            f"{self.base_url}/api/flats/{flat_id}/similar",
            params={"area_tolerance": area_tolerance, "min_flats": min_flats},
        )
        response.raise_for_status()
        return response.json()

    def get_database_stats(self) -> Dict:
        """Get database statistics."""
        response = requests.get(f"{self.base_url}/api/database/stats", timeout=5)
        response.raise_for_status()
        return response.json()

    def get_jk_sales_analysis(
        self, jk_name: str, discount_percentage: float = 0.15
    ) -> SalesAnalysisResponse:
        """Get JK sales analysis."""
        response = requests.get(
            f"{self.base_url}/api/jks/sales/{jk_name}/analysis",
            params={"discount_percentage": discount_percentage},
        )
        response.raise_for_status()
        data = response.json()

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
        self, jk_name: str, min_yield_percentage: float = 0.05
    ) -> RentalAnalysisResponse:
        """Get JK rentals analysis."""
        response = requests.get(
            f"{self.base_url}/api/jks/rentals/{jk_name}/analysis",
            params={"min_yield_percentage": min_yield_percentage},
        )
        response.raise_for_status()
        data = response.json()

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
