"""
JK (Residential Complex) sales analysis endpoints.
"""
from fastapi import APIRouter, HTTPException, Query
from typing import List, Optional
import logging
import sys
import os

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from analytics.src.jk_analytics import JKAnalytics, analyze_jk_for_sales

logger = logging.getLogger(__name__)
router = APIRouter()

# Initialize analytics
analytics = JKAnalytics("flats.db")

@router.get("/")
async def get_jk_list():
    """
    Get list of all residential complexes (JKs).
    """
    try:
        jks = analytics.get_jk_list()
        return {
            "success": True,
            "jks": jks,
            "count": len(jks)
        }
    except Exception as e:
        logger.error(f"Error getting JK list: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/{jk_name}/summary")
async def get_jk_sales_summary(jk_name: str):
    """
    Get sales summary for a specific residential complex.
    """
    try:
        summary = analytics.get_jk_sales_summary(jk_name)
        return {
            "success": True,
            "jk_name": jk_name,
            "summary": summary
        }
    except Exception as e:
        logger.error(f"Error getting sales summary for JK {jk_name}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/{jk_name}/analysis")
async def get_jk_sales_analysis(
    jk_name: str,
    discount_percentage: float = Query(0.15, description="Discount percentage threshold for opportunities")
):
    """
    Get comprehensive sales analysis for a residential complex.
    """
    try:
        analysis = analyze_jk_for_sales(jk_name, discount_percentage)
        
        # Convert opportunities to JSON-serializable format
        opportunities_by_type = analysis['current_market'].opportunities
        serialized_opportunities = {}
        
        for flat_type, opportunities in opportunities_by_type.items():
            serialized_opportunities[flat_type] = [
                {
                    "flat_id": opp.flat_info.flat_id,
                    "price": opp.flat_info.price,
                    "area": opp.flat_info.area,
                    "flat_type": opp.flat_info.flat_type,
                    "floor": opp.flat_info.floor,
                    "total_floors": opp.flat_info.total_floors,
                    "residential_complex": opp.flat_info.residential_complex,
                    "construction_year": opp.flat_info.construction_year,
                    "parking": opp.flat_info.parking,
                    "description": opp.flat_info.description,
                    "discount_percentage_vs_median": opp.discount_percentage_vs_median,
                    "market_stats": {
                        "residential_complex": opp.stats_for_flat_type.residential_complex,
                        "mean_price": opp.stats_for_flat_type.mean_price,
                        "median_price": opp.stats_for_flat_type.median_price,
                        "min_price": opp.stats_for_flat_type.min_price,
                        "max_price": opp.stats_for_flat_type.max_price,
                        "count": opp.stats_for_flat_type.count
                    },
                    "query_date": opp.query_date
                } for opp in opportunities
            ]
        
        return {
            "success": True,
            "jk_name": jk_name,
            "discount_percentage": discount_percentage,
            "current_market": {
                "global_stats": {
                    "mean": analysis['current_market'].global_stats.mean,
                    "median": analysis['current_market'].global_stats.median,
                    "min": analysis['current_market'].global_stats.min,
                    "max": analysis['current_market'].global_stats.max,
                    "count": analysis['current_market'].global_stats.count
                },
                "flat_type_buckets": {
                    flat_type: {
                        "mean": stats.mean,
                        "median": stats.median,
                        "min": stats.min,
                        "max": stats.max,
                        "count": stats.count
                    } for flat_type, stats in analysis['current_market'].flat_type_buckets.items()
                },
                "opportunities": serialized_opportunities
            },
            "historical_analysis": {
                "timeseries_data": [
                    {
                        "date": point.date,
                        "flat_type": point.flat_type,
                        "residential_complex": point.residential_complex,
                        "mean_price": point.mean_price,
                        "median_price": point.median_price,
                        "min_price": point.min_price,
                        "max_price": point.max_price,
                        "count": point.count
                    } for point in analysis['historical_analysis'].timeseries_data
                ]
            }
        }
    except Exception as e:
        logger.error(f"Error getting sales analysis for JK {jk_name}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/{jk_name}/opportunities")
async def get_jk_sales_opportunities(
    jk_name: str,
    discount_percentage: float = Query(0.15, description="Discount percentage threshold"),
    limit: int = Query(10, description="Maximum number of opportunities to return")
):
    """
    Get sales opportunities for a residential complex.
    """
    try:
        analysis = analyze_jk_for_sales(jk_name, discount_percentage)
        opportunities_by_type = analysis['current_market'].opportunities
        
        # Flatten all opportunities and sort by discount percentage
        all_opportunities = []
        for flat_type, opportunities in opportunities_by_type.items():
            for opp in opportunities:
                all_opportunities.append({
                    "flat_id": opp.flat_info.flat_id,
                    "price": opp.flat_info.price,
                    "area": opp.flat_info.area,
                    "flat_type": opp.flat_info.flat_type,
                    "floor": opp.flat_info.floor,
                    "total_floors": opp.flat_info.total_floors,
                    "residential_complex": opp.flat_info.residential_complex,
                    "construction_year": opp.flat_info.construction_year,
                    "parking": opp.flat_info.parking,
                    "description": opp.flat_info.description,
                    "discount_percentage_vs_median": opp.discount_percentage_vs_median,
                    "market_stats": {
                        "residential_complex": opp.stats_for_flat_type.residential_complex,
                        "mean_price": opp.stats_for_flat_type.mean_price,
                        "median_price": opp.stats_for_flat_type.median_price,
                        "min_price": opp.stats_for_flat_type.min_price,
                        "max_price": opp.stats_for_flat_type.max_price,
                        "count": opp.stats_for_flat_type.count
                    },
                    "query_date": opp.query_date
                })
        
        # Sort by discount percentage (highest first) and limit
        all_opportunities.sort(key=lambda x: x['discount_percentage_vs_median'], reverse=True)
        limited_opportunities = all_opportunities[:limit]
        
        return {
            "success": True,
            "jk_name": jk_name,
            "discount_percentage": discount_percentage,
            "opportunities": limited_opportunities,
            "total_found": len(all_opportunities)
        }
    except Exception as e:
        logger.error(f"Error getting sales opportunities for JK {jk_name}: {e}")
        raise HTTPException(status_code=500, detail=str(e))
