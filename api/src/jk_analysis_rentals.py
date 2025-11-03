"""
JK (Residential Complex) rentals analysis endpoints.
"""

import logging
from typing import Optional

from fastapi import APIRouter, HTTPException, Query

from analytics.src.jk_rentals_analytics import JKRentalAnalytics, analyze_jk_for_rentals
from api.src.analysis_objects import (
    RentalAnalysisResponse,
    RentalCurrentMarket,
    RentalGlobalStats,
    RentalFlatTypeStats,
    Opportunity,
    RentalHistoricalAnalysis,
    RentalHistoricalPoint,
)
from db.src.write_read_database import OrthancDB

logger = logging.getLogger(__name__)
router = APIRouter()

analytics_engine = JKRentalAnalytics()


@router.get("/")
async def get_jks_with_rental_data():
    """
    Get a list of all residential complexes (JKs) with rental data.
    """
    try:
        jks = analytics_engine.get_jk_list()
        return {"success": True, "jks": jks}
    except Exception as e:
        logger.error(f"Error getting JK list: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{jk_name}/summary")
async def get_jk_rentals_summary(jk_name: str):
    """
    Get a summary of rental data for a specific residential complex.
    """
    try:
        summary = analytics_engine.get_jk_rentals_summary(jk_name)
        if not summary:
            raise HTTPException(
                status_code=404, detail=f"No rental summary found for JK: {jk_name}"
            )
        return {"success": True, "jk_name": jk_name, "summary": summary}
    except Exception as e:
        logger.error(f"Error getting rental summary for JK {jk_name}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{jk_name}/analysis")
async def get_jk_rentals_analysis(
    jk_name: str,
    min_yield_percentage: float = Query(
        0.05, description="Minimum yield percentage threshold for opportunities"
    ),
) -> RentalAnalysisResponse:
    """
    Get comprehensive rental analysis for a residential complex.
    """
    analysis = analyze_jk_for_rentals(jk_name, min_yield_percentage)

    # Convert opportunities to objects
    opportunities_by_type = analysis["current_market"].opportunities
    opportunity_objects = {}

    for flat_type, opportunities in opportunities_by_type.items():
        opportunity_objects[flat_type] = [
            Opportunity(
                flat_id=opp.flat_info.flat_id,
                price=opp.flat_info.price,
                area=opp.flat_info.area,
                flat_type=opp.flat_info.flat_type,
                residential_complex=opp.flat_info.residential_complex,
                floor=opp.flat_info.floor,
                total_floors=opp.flat_info.total_floors,
                construction_year=opp.flat_info.construction_year,
                parking=opp.flat_info.parking,
                description=opp.flat_info.description,
                is_rental=opp.flat_info.is_rental,
                yield_percentage=opp.yield_percentage,
                market_stats={
                    "residential_complex": opp.stats_for_flat_type.residential_complex,
                    "mean_yield": opp.stats_for_flat_type.mean_yield,
                    "median_yield": opp.stats_for_flat_type.median_yield,
                    "min_yield": opp.stats_for_flat_type.min_yield,
                    "max_yield": opp.stats_for_flat_type.max_yield,
                    "count": opp.stats_for_flat_type.count,
                },
                query_date=opp.query_date,
            )
            for opp in opportunities
        ]

    # Create global stats object
    global_stats = RentalGlobalStats(
        mean_yield=analysis["current_market"].global_stats.mean_yield,
        median_yield=analysis["current_market"].global_stats.median_yield,
        min_yield=analysis["current_market"].global_stats.min_yield,
        max_yield=analysis["current_market"].global_stats.max_yield,
        count=analysis["current_market"].global_stats.count,
    )

    # Create flat type buckets objects
    flat_type_buckets = {
        flat_type: RentalFlatTypeStats(
            mean_yield=stats.mean_yield,
            median_yield=stats.median_yield,
            min_yield=stats.min_yield,
            max_yield=stats.max_yield,
            count=stats.count,
        )
        for flat_type, stats in analysis["current_market"].flat_type_buckets.items()
    }

    # Create current market object
    current_market = RentalCurrentMarket(
        global_stats=global_stats,
        flat_type_buckets=flat_type_buckets,
        opportunities=opportunity_objects,
    )

    # Create historical analysis objects
    historical_points = {}
    for flat_type, points in analysis[
        "historical_analysis"
    ].flat_type_timeseries.items():
        historical_points[flat_type] = [
            RentalHistoricalPoint(
                date=point.date,
                flat_type=point.flat_type,
                residential_complex=point.residential_complex,
                mean_rental=point.mean_rental,
                median_rental=point.median_rental,
                min_rental=point.min_rental,
                max_rental=point.max_rental,
                mean_yield=point.mean_yield,
                median_yield=point.median_yield,
                count=point.count,
            )
            for point in points
        ]

    historical_analysis = RentalHistoricalAnalysis(
        flat_type_timeseries=historical_points
    )

    return RentalAnalysisResponse(
        success=True,
        jk_name=jk_name,
        min_yield_percentage=min_yield_percentage,
        current_market=current_market,
        historical_analysis=historical_analysis,
    )


@router.get("/{jk_name}/opportunities")
async def get_jk_rentals_opportunities(
    jk_name: str,
    min_yield_percentage: float = Query(
        0.05, description="Minimum yield percentage threshold for opportunities"
    ),
    limit: int = Query(10, description="Maximum number of opportunities to return"),
):
    """
    Get a list of rental opportunities for a specific residential complex.
    """
    analysis = analyze_jk_for_rentals(jk_name, min_yield_percentage)

    # Flatten all opportunities and sort by yield percentage
    all_opportunities = []
    for flat_type, opportunities in analysis["current_market"].opportunities.items():
        for opp in opportunities:
            all_opportunities.append(
                {
                    "flat_id": opp.flat_info.flat_id,
                    "price": opp.flat_info.price,
                    "area": opp.flat_info.area,
                    "flat_type": opp.flat_info.flat_type,
                    "residential_complex": opp.flat_info.residential_complex,
                    "floor": opp.flat_info.floor,
                    "total_floors": opp.flat_info.total_floors,
                    "construction_year": opp.flat_info.construction_year,
                    "parking": opp.flat_info.parking,
                    "description": opp.flat_info.description,
                    "is_rental": opp.flat_info.is_rental,
                    "yield_percentage": opp.yield_percentage,
                    "query_date": opp.query_date,
                }
            )

    # Sort by yield percentage (highest first) and limit
    all_opportunities.sort(key=lambda x: x["yield_percentage"], reverse=True)
    limited_opportunities = all_opportunities[:limit]

    return {
        "success": True,
        "jk_name": jk_name,
        "min_yield_percentage": min_yield_percentage,
        "opportunities": limited_opportunities,
        "count": len(limited_opportunities),
        "total_found": len(all_opportunities),
    }


@router.get("/{jk_name}/rentals")
async def get_jk_rentals(
    jk_name: str,
    flat_type: Optional[str] = Query(None, description="Filter by flat type"),
    min_price: Optional[int] = Query(None, description="Minimum price filter"),
    max_price: Optional[int] = Query(None, description="Maximum price filter"),
    limit: int = Query(50, description="Maximum number of results"),
):
    """
    Get rental listings for a specific residential complex.
    """
    db = OrthancDB("flats.db")
    db.connect()

    query = "SELECT * FROM rental_flats WHERE residential_complex = ?"
    params = [jk_name]

    if flat_type:
        query += " AND flat_type = ?"
        params.append(flat_type)

    if min_price:
        query += " AND price >= ?"
        params.append(min_price)

    if max_price:
        query += " AND price <= ?"
        params.append(max_price)

    query += " ORDER BY price ASC LIMIT ?"
    params.append(limit)

    cursor = db.conn.execute(query, params)
    rentals = [dict(row) for row in cursor.fetchall()]

    db.disconnect()

    return {
        "success": True,
        "jk_name": jk_name,
        "rentals": rentals,
        "count": len(rentals),
    }


@router.get("/{jk_name}/price-trends")
async def get_jk_rental_price_trends(jk_name: str):
    """
    Get rental price trends for a residential complex over time.
    """
    db = OrthancDB("flats.db")
    db.connect()

    # Get price trends by date
    cursor = db.conn.execute(
        """
        SELECT 
            DATE(query_date) as date,
            flat_type,
            COUNT(*) as count,
            AVG(price) as avg_price,
            MIN(price) as min_price,
            MAX(price) as max_price
        FROM rental_flats 
        WHERE residential_complex = ?
        GROUP BY DATE(query_date), flat_type
        ORDER BY date DESC, flat_type
    """,
        (jk_name,),
    )

    trends = [dict(row) for row in cursor.fetchall()]

    db.disconnect()

    return {"success": True, "jk_name": jk_name, "price_trends": trends}


@router.get("/stats/overview")
async def get_rentals_overview():
    """
    Get overall rental market overview.
    """
    db = OrthancDB("flats.db")
    db.connect()

    # Overall rental stats
    cursor = db.conn.execute("""
        SELECT 
            COUNT(*) as total_rentals,
            COUNT(DISTINCT residential_complex) as total_jks,
            AVG(price) as avg_price,
            MIN(price) as min_price,
            MAX(price) as max_price,
            AVG(area) as avg_area
        FROM rental_flats
    """)

    overall_stats = dict(cursor.fetchone())

    # Top JKs by rental count
    cursor = db.conn.execute("""
        SELECT 
            residential_complex,
            COUNT(*) as rental_count,
            AVG(price) as avg_price
        FROM rental_flats 
        WHERE residential_complex IS NOT NULL
        GROUP BY residential_complex
        ORDER BY rental_count DESC
        LIMIT 10
    """)

    top_jks = [dict(row) for row in cursor.fetchall()]

    # Flat type distribution
    cursor = db.conn.execute("""
        SELECT 
            flat_type,
            COUNT(*) as count,
            AVG(price) as avg_price
        FROM rental_flats
        GROUP BY flat_type
        ORDER BY count DESC
    """)

    flat_type_distribution = [dict(row) for row in cursor.fetchall()]

    db.disconnect()

    return {
        "success": True,
        "overview": {
            "overall_stats": overall_stats,
            "top_jks": top_jks,
            "flat_type_distribution": flat_type_distribution,
        },
    }
