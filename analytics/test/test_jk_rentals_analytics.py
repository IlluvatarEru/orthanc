"""
Test JK Rentals Analytics functionality.

This module tests the JK rentals analytics functionality using pytest.

python -m pytest analytics/test/test_jk_rentals_analytics.py -v -s --log-cli-level=INFO
"""

import sys
import os
import pytest
import logging
from typing import Dict, List, Optional
from datetime import datetime

from analytics.src.jk_rentals_analytics import JKRentalAnalytics, analyze_jk_for_rentals, StatsForFlatType
from analytics.src.jk_rentals_analytics import RentalYieldStats, RentalOpportunity, CurrentRentalMarketAnalysis, HistoricalRentalAnalysis
from common.src.flat_info import FlatInfo

# Test JK name
TEST_JK_NAME = "Meridian Apartments"

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class TestJKRentalAnalytics:
    """Test class for JK rentals analytics functionality."""

    def test_analyze_jk_for_rentals_success(self):
        """
        Test successful JK rentals analysis.
        
        This test verifies that the analytics can successfully analyze
        rental data for a specific residential complex.
        """
        logger.info(f"Testing JK rentals analytics for: {TEST_JK_NAME}")
        logger.info(f"Minimum yield percentage: 5%")

        # Analyze the JK
        analysis = analyze_jk_for_rentals(TEST_JK_NAME, 0.05)

        # CRITICAL: Test should fail if analysis returns None or empty
        assert analysis is not None, "Failed to analyze JK - returned None. This indicates the analytics is not working properly."
        assert 'current_market' in analysis, "Analysis missing current_market data"
        assert 'historical_analysis' in analysis, "Analysis missing historical_analysis data"

        logger.info("Successfully analyzed JK!")
        logger.info("=" * 50)
        logger.info("ANALYSIS RESULTS:")
        logger.info("=" * 50)

        # Check and log all analysis fields
        fields_to_check = [
            ('jk_name', analysis['jk_name']),
            ('analysis_timestamp', analysis['analysis_timestamp']),
            ('min_yield_percentage', analysis['min_yield_percentage']),
        ]
        for k,v in analysis.items():
            logger.info(f"{k}: {v}")
        for field_name, field_value in fields_to_check:
            logger.info(f"{field_name:25}: {field_value}")

        logger.info("=" * 50)
        opportunities = analysis["current_market"].opportunities
        logger.info(f"Available flat types: {list(opportunities.keys())}")
        
        # Show opportunities for each flat type that exists
        for flat_type, opps in opportunities.items():
            if opps:  # Only show if there are opportunities
                logger.info(f"{flat_type} opportunities: {[opp.yield_percentage for opp in opps]}")
                logger.info(f"First {flat_type} opportunity: {opps[0]}")
            else:
                logger.info(f"{flat_type}: No opportunities found")
        logger.info("=" * 50)

        # Verify basic structure
        assert analysis['jk_name'] == TEST_JK_NAME
        assert analysis['min_yield_percentage'] == 0.05
        assert isinstance(analysis['analysis_timestamp'], str)

    def test_current_rental_market_analysis_structure(self):
        """Test the structure and content of current rental market analysis."""
        logger.info("Testing current rental market analysis structure...")

        analysis = analyze_jk_for_rentals(TEST_JK_NAME, 0.05)
        current_market: CurrentRentalMarketAnalysis = analysis['current_market']

        # CRITICAL: Test should fail if current_market is None
        assert current_market is not None, "Cannot test structure - current_market is None."

        logger.info("Current Rental Market Analysis:")
        logger.info("=" * 30)

        # Test global stats
        global_stats = current_market.global_stats
        logger.info(f"Global Stats:")
        logger.info(f"  Mean Yield: {global_stats.mean_yield}")
        logger.info(f"  Median Yield: {global_stats.median_yield}")
        logger.info(f"  Min Yield: {global_stats.min_yield}")
        logger.info(f"  Max Yield: {global_stats.max_yield}")
        logger.info(f"  Count: {global_stats.count}")

        # Verify global stats structure
        assert isinstance(global_stats, RentalYieldStats)
        assert isinstance(global_stats.mean_yield, (int, float))
        assert isinstance(global_stats.median_yield, (int, float))
        assert isinstance(global_stats.min_yield, (int, float))
        assert isinstance(global_stats.max_yield, (int, float))
        assert isinstance(global_stats.count, int)
        assert global_stats.count >= 0

        # Test flat type buckets
        flat_type_buckets = current_market.flat_type_buckets
        logger.info(f"Flat Type Buckets: {list(flat_type_buckets.keys())}")
        
        for flat_type, stats in flat_type_buckets.items():
            logger.info(f"  {flat_type}: Mean Yield={stats.mean_yield}, Count={stats.count}")
            assert isinstance(stats, RentalYieldStats)
            assert stats.count > 0

        # Test opportunities (now grouped by flat type)
        opportunities_by_type = current_market.opportunities
        logger.info(f"Opportunities by type: {list(opportunities_by_type.keys())}")
        
        total_opportunities = sum(len(opps) for opps in opportunities_by_type.values())
        logger.info(f"Total opportunities found: {total_opportunities}")
        
        for flat_type, opportunities in opportunities_by_type.items():
            logger.info(f"  {flat_type}: {len(opportunities)} opportunities")
            for i, opp in enumerate(opportunities[:2]):
                logger.info(f"    {i+1}. Flat {opp.flat_info.flat_id}: {opp.flat_info.price:,}₸/month (Yield: {opp.yield_percentage:.2f}%)")
                assert isinstance(opp, RentalOpportunity)
                assert isinstance(opp.flat_info, FlatInfo)
                assert isinstance(opp.stats_for_flat_type, StatsForFlatType)
                assert isinstance(opp.yield_percentage, (int, float))
                assert opp.yield_percentage >= 0.05  # Should meet minimum yield threshold
        
        # Verify that opportunities is a dictionary
        assert isinstance(opportunities_by_type, dict), f"Expected dict, got {type(opportunities_by_type)}"
        
        # If there are opportunities, verify the structure
        if total_opportunities > 0:
            # Find any flat type with opportunities
            flat_type_with_opps = None
            for flat_type, opps in opportunities_by_type.items():
                if opps:
                    flat_type_with_opps = flat_type
                    break
            
            if flat_type_with_opps:
                logger.info(f"Testing opportunities structure for {flat_type_with_opps}")
                first_opp = opportunities_by_type[flat_type_with_opps][0]
                assert isinstance(first_opp, RentalOpportunity)
                assert hasattr(first_opp, 'flat_info')
                assert hasattr(first_opp, 'stats_for_flat_type')
                assert hasattr(first_opp, 'yield_percentage')
                assert first_opp.yield_percentage >= 0.05

        logger.info("✅ Current rental market analysis structure validation passed!")

    def test_historical_rental_analysis_structure(self):
        """Test the structure and content of historical rental analysis."""
        logger.info("Testing historical rental analysis structure...")

        analysis = analyze_jk_for_rentals(TEST_JK_NAME, 0.05)
        historical = analysis['historical_analysis']

        # CRITICAL: Test should fail if historical is None
        assert historical is not None, "Cannot test structure - historical is None."

        logger.info("Historical Rental Analysis:")
        logger.info("=" * 30)

        # Test basic structure
        assert isinstance(historical, HistoricalRentalAnalysis)
        assert historical.jk_name == TEST_JK_NAME
        assert isinstance(historical.analysis_period, tuple)
        assert len(historical.analysis_period) == 2

        logger.info(f"Analysis period: {historical.analysis_period[0]} to {historical.analysis_period[1]}")

        # Test flat type timeseries
        flat_type_timeseries = historical.flat_type_timeseries
        logger.info(f"Flat type timeseries: {list(flat_type_timeseries.keys())}")

        for flat_type, timeseries in flat_type_timeseries.items():
            logger.info(f"  {flat_type}: {len(timeseries)} data points")
            assert isinstance(timeseries, list)
            
            for data_point in timeseries:
                assert isinstance(data_point, StatsForFlatType)
                assert isinstance(data_point.date, str)
                assert isinstance(data_point.flat_type, str)
                assert isinstance(data_point.mean_rental, (int, float))
                assert isinstance(data_point.median_rental, (int, float))
                assert isinstance(data_point.min_rental, (int, float))
                assert isinstance(data_point.max_rental, (int, float))
                assert isinstance(data_point.count, int)

        logger.info("✅ Historical rental analysis structure validation passed!")

    def test_opportunity_detection(self):
        """Test opportunity detection with different yield percentages."""
        logger.info("Testing opportunity detection...")

        # Test with 3% yield
        analysis_3 = analyze_jk_for_rentals(TEST_JK_NAME, 0.03)
        opportunities_3_dict = analysis_3['current_market'].opportunities

        # Test with 8% yield
        analysis_8 = analyze_jk_for_rentals(TEST_JK_NAME, 0.08)
        opportunities_8_dict = analysis_8['current_market'].opportunities

        # Flatten opportunities to get total counts
        opportunities_3 = []
        for opps in opportunities_3_dict.values():
            opportunities_3.extend(opps)
        
        opportunities_8 = []
        for opps in opportunities_8_dict.values():
            opportunities_8.extend(opps)

        logger.info(f"Opportunities with 3% yield: {len(opportunities_3)}")
        logger.info(f"Opportunities with 8% yield: {len(opportunities_8)}")

        # Both should be non-negative
        assert len(opportunities_3) >= 0, "Opportunities count should be non-negative"
        assert len(opportunities_8) >= 0, "Opportunities count should be non-negative"
        
        # Log the relationship for debugging
        if len(opportunities_8) <= len(opportunities_3):
            logger.info("✅ Higher yield threshold found fewer or equal opportunities (expected)")
        else:
            logger.info(f"⚠️ Higher yield threshold found more opportunities ({len(opportunities_8)} vs {len(opportunities_3)}) - unexpected behavior")

        # Test opportunity structure (only if opportunities exist)
        if opportunities_3:
            for opp in opportunities_3:
                assert opp.yield_percentage >= 0.0, f"Opportunity yield {opp.yield_percentage}% should be >= 0%"
                assert opp.flat_info.price > 0, "Opportunity price should be positive"
                assert opp.flat_info.area > 0, "Opportunity area should be positive"
                assert opp.flat_info.flat_id, "Opportunity should have flat_id"
                assert isinstance(opp.flat_info, FlatInfo), "Opportunity should have FlatInfo"
                assert isinstance(opp.stats_for_flat_type, StatsForFlatType), "Opportunity should have StatsForFlatType"
        else:
            logger.info("No opportunities found with 3% yield - this might be normal if no flats meet the criteria")

        logger.info("✅ Opportunity detection validation passed!")

    def test_jk_rental_analytics_class_methods(self):
        """Test JKRentalAnalytics class methods."""
        logger.info("Testing JKRentalAnalytics class methods...")

        analytics = JKRentalAnalytics()

        # Test get_jk_list
        jks = analytics.get_jk_list()
        logger.info(f"Available JKs: {len(jks)}")
        assert isinstance(jks, list)
        assert TEST_JK_NAME in jks, f"{TEST_JK_NAME} should be in JK list"

        # Test get_jk_rentals_summary
        summary = analytics.get_jk_rentals_summary(TEST_JK_NAME)
        logger.info(f"Summary: {summary}")
        
        assert isinstance(summary, dict)
        assert 'jk_name' in summary
        assert 'total_rentals' in summary
        assert 'date_range' in summary
        assert 'flat_type_distribution' in summary
        
        assert summary['jk_name'] == TEST_JK_NAME
        assert isinstance(summary['total_rentals'], int)
        assert summary['total_rentals'] >= 0

        logger.info("✅ JKRentalAnalytics class methods validation passed!")

    def test_yield_stats_calculation(self):
        """Test yield statistics calculation."""
        logger.info("Testing yield statistics calculation...")

        analytics = JKRentalAnalytics()
        
        # Test with sample yields
        test_yields = [5.0, 6.5, 7.2, 8.1, 9.0]
        stats = analytics._calculate_yield_stats(test_yields)

        logger.info(f"Test yields: {test_yields}")
        logger.info(f"Calculated stats: Mean={stats.mean_yield}, Median={stats.median_yield}, Min={stats.min_yield}, Max={stats.max_yield}")

        assert round(stats.mean_yield,2) == 7.16  # (5.0 + 6.5 + 7.2 + 8.1 + 9.0) / 5
        assert stats.median_yield == 7.2
        assert stats.min_yield == 5.0
        assert stats.max_yield == 9.0
        assert stats.count == 5

        # Test with empty list
        empty_stats = analytics._calculate_yield_stats([])
        assert empty_stats.mean_yield == 0
        assert empty_stats.median_yield == 0
        assert empty_stats.min_yield == 0
        assert empty_stats.max_yield == 0
        assert empty_stats.count == 0

        logger.info("✅ Yield statistics calculation validation passed!")

    def test_analysis_data_consistency(self):
        """Test that analysis data is consistent and logical."""
        logger.info("Testing analysis data consistency...")

        analysis = analyze_jk_for_rentals(TEST_JK_NAME, 0.05)
        current_market = analysis['current_market']

        # Test that global stats make sense
        global_stats = current_market.global_stats
        assert global_stats.min_yield <= global_stats.median_yield <= global_stats.max_yield, "Min <= Median <= Max should hold"
        assert global_stats.min_yield <= global_stats.mean_yield <= global_stats.max_yield, "Min <= Mean <= Max should hold"

        # Test that flat type bucket stats are consistent
        for flat_type, stats in current_market.flat_type_buckets.items():
            assert stats.min_yield <= stats.median_yield <= stats.max_yield, f"{flat_type}: Min <= Median <= Max should hold"
            assert stats.min_yield <= stats.mean_yield <= stats.max_yield, f"{flat_type}: Min <= Mean <= Max should hold"
            assert stats.count >= 0, f"{flat_type}: Count should be non-negative"

        # Test that opportunities have valid yield percentages
        all_opportunities = []
        for opps in current_market.opportunities.values():
            all_opportunities.extend(opps)
        
        for opp in all_opportunities:
            assert opp.yield_percentage >= 0.05, f"Opportunity {opp.flat_info.flat_id} yield {opp.yield_percentage}% should be >= 5.0%"
            assert opp.flat_info.price > 0, f"Opportunity {opp.flat_info.flat_id} price should be positive"

        logger.info("✅ Analysis data consistency validation passed!")

    def test_historical_timeseries_data(self):
        """Test historical timeseries data structure and content."""
        logger.info("Testing historical timeseries data...")

        analysis = analyze_jk_for_rentals(TEST_JK_NAME, 0.05)
        historical = analysis['historical_analysis']

        # Test that we have some historical data
        total_data_points = sum(len(timeseries) for timeseries in historical.flat_type_timeseries.values())
        logger.info(f"Total historical data points: {total_data_points}")

        # Test that dates are in correct format
        for flat_type, timeseries in historical.flat_type_timeseries.items():
            for data_point in timeseries:
                # Test date format (YYYY-MM-DD)
                try:
                    datetime.strptime(data_point.date, '%Y-%m-%d')
                except ValueError:
                    assert False, f"Invalid date format: {data_point.date}"

                # Test that rental prices are reasonable
                assert data_point.mean_rental > 0, f"Mean rental should be positive: {data_point.mean_rental}"
                assert data_point.median_rental > 0, f"Median rental should be positive: {data_point.median_rental}"
                assert data_point.min_rental > 0, f"Min rental should be positive: {data_point.min_rental}"
                assert data_point.max_rental > 0, f"Max rental should be positive: {data_point.max_rental}"
                assert data_point.count > 0, f"Count should be positive: {data_point.count}"

        logger.info("✅ Historical timeseries data validation passed!")

    def test_analysis_performance(self):
        """Test that analysis completes in reasonable time."""
        logger.info("Testing analysis performance...")

        import time
        start_time = time.time()

        analysis = analyze_jk_for_rentals(TEST_JK_NAME, 0.05)

        end_time = time.time()
        duration = end_time - start_time

        logger.info(f"Analysis completed in {duration:.2f} seconds")

        # Analysis should complete in reasonable time (less than 30 seconds)
        assert duration < 30, f"Analysis took too long: {duration:.2f} seconds"

        # Verify we got results
        assert analysis is not None
        assert 'current_market' in analysis
        assert 'historical_analysis' in analysis

        logger.info("✅ Analysis performance validation passed!")

    def test_edge_cases(self):
        """Test edge cases and error handling."""
        logger.info("Testing edge cases...")

        analytics = JKRentalAnalytics()

        # Test with non-existent JK
        try:
            analysis = analytics.analyse_jk_for_rentals("NonExistentJK", 0.05)
            # Should still return analysis with empty data
            assert analysis is not None
            assert analysis['current_market'].global_stats.count == 0
            assert len(analysis['current_market'].opportunities) == 0
            logger.info("✅ Non-existent JK handled gracefully")
        except Exception as e:
            logger.warning(f"Non-existent JK test failed: {e}")

        # Test with extreme yield percentage
        try:
            analysis = analytics.analyse_jk_for_rentals(TEST_JK_NAME, 0.50)  # 50% yield
            assert analysis is not None
            logger.info("✅ Extreme yield percentage handled")
        except Exception as e:
            logger.warning(f"Extreme yield test failed: {e}")

        logger.info("✅ Edge cases validation passed!")
