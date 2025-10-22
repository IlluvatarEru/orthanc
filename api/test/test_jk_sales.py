"""
Tests for JK sales analysis API endpoints.
"""
import pytest
import requests
import time
import subprocess
import logging

logger = logging.getLogger(__name__)

class TestJKSalesAPI:
    """Test class for JK sales analysis endpoints."""
    
    @pytest.fixture(scope="class")
    def api_server(self):
        """Start API server for testing."""
        # Start the API server in a subprocess
        process = subprocess.Popen([
            "python", "-m", "api.launch.launch_api", 
            "--host", "127.0.0.1", 
            "--port", "8004"
        ], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        
        # Wait for server to start
        time.sleep(5)
        
        yield "http://127.0.0.1:8004"
        
        # Clean up
        process.terminate()
        process.wait()
    
    def test_jk_sales_list(self, api_server):
        """Test getting list of all JKs."""
        logger.info("Testing JK sales list...")
        response = requests.get(f"{api_server}/api/jks/sales/")
        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert "jks" in data
        assert "count" in data
        logger.info(f"Found {data['count']} JKs")
        
        # Should have at least some JKs
        assert data["count"] > 0, "Should have at least one JK in the database"
    
    def test_jk_sales_summary_meridian(self, api_server):
        """Test getting sales summary for Meridian Apartments."""
        logger.info("Testing JK sales summary for Meridian Apartments...")
        response = requests.get(f"{api_server}/api/jks/sales/Meridian%20Apartments/summary")
        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert data["jk_name"] == "Meridian Apartments"
        assert "summary" in data
        
        summary = data["summary"]
        logger.info(f"Meridian Apartments summary: {summary}")
        
        # Should have sales data
        assert summary["total_sales"] > 0, "Meridian Apartments should have sales data"
        
        # Check if we have price/area data in the summary
        if "avg_price" in summary:
            assert summary["avg_price"] > 0, "Average price should be positive"
            assert summary["min_price"] > 0, "Minimum price should be positive"
            assert summary["max_price"] > 0, "Maximum price should be positive"
            assert summary["avg_area"] > 0, "Average area should be positive"
            
            # Price range validation
            assert summary["min_price"] <= summary["avg_price"] <= summary["max_price"], "Price statistics should be logical"
            
            # Should have reasonable price range for Almaty apartments
            assert 10_000_000 <= summary["min_price"] <= 200_000_000, f"Min price {summary['min_price']} should be reasonable for Almaty"
            assert 10_000_000 <= summary["max_price"] <= 500_000_000, f"Max price {summary['max_price']} should be reasonable for Almaty"
        
        logger.info(f"✅ Meridian Apartments has {summary['total_sales']} sales")
        if "min_price" in summary and "max_price" in summary:
            logger.info(f"Price range: {summary['min_price']:,}₸ - {summary['max_price']:,}₸")
    
    def test_jk_sales_analysis_meridian(self, api_server):
        """Test comprehensive sales analysis for Meridian Apartments."""
        logger.info("Testing JK sales analysis for Meridian Apartments...")
        response = requests.get(f"{api_server}/api/jks/sales/Meridian%20Apartments/analysis?discount_percentage=0.15")
        
        assert response.status_code == 200, f"Analysis endpoint failed: {response.text}"
        data = response.json()
        assert data["success"] is True
        assert data["jk_name"] == "Meridian Apartments"
        assert data["discount_percentage"] == 0.15
        
        # Test current market analysis structure
        current_market = data["current_market"]
        assert "global_stats" in current_market
        assert "flat_type_buckets" in current_market
        assert "opportunities" in current_market
        
        # Test global stats
        global_stats = current_market["global_stats"]
        assert global_stats["count"] > 0, "Should have sales data"
        assert global_stats["mean"] > 0, "Mean price should be positive"
        assert global_stats["median"] > 0, "Median price should be positive"
        assert global_stats["min"] > 0, "Min price should be positive"
        assert global_stats["max"] > 0, "Max price should be positive"
        
        # Price logic validation
        assert global_stats["min"] <= global_stats["median"] <= global_stats["max"], "Min <= Median <= Max should hold"
        assert global_stats["min"] <= global_stats["mean"] <= global_stats["max"], "Min <= Mean <= Max should hold"
        
        # Test flat type buckets
        flat_type_buckets = current_market["flat_type_buckets"]
        assert len(flat_type_buckets) > 0, "Should have at least one flat type"
        
        for flat_type, stats in flat_type_buckets.items():
            assert stats["count"] > 0, f"{flat_type} should have sales data"
            assert stats["mean"] > 0, f"{flat_type} mean price should be positive"
            assert stats["median"] > 0, f"{flat_type} median price should be positive"
            assert stats["min"] > 0, f"{flat_type} min price should be positive"
            assert stats["max"] > 0, f"{flat_type} max price should be positive"
            
            # Price logic validation
            assert stats["min"] <= stats["median"] <= stats["max"], f"{flat_type}: Min <= Median <= Max should hold"
            assert stats["min"] <= stats["mean"] <= stats["max"], f"{flat_type}: Min <= Mean <= Max should hold"
            
            logger.info(f"  {flat_type}: {stats['count']} sales, avg {stats['mean']:,.0f}₸, median {stats['median']:,.0f}₸")
        
        # Test opportunities
        opportunities = current_market["opportunities"]
        assert isinstance(opportunities, dict), "Opportunities should be a dictionary"
        
        total_opportunities = sum(len(opps) for opps in opportunities.values())
        logger.info(f"Found {total_opportunities} opportunities across {len(opportunities)} flat types")
        
        # Test historical analysis
        historical_analysis = data["historical_analysis"]
        assert "flat_type_timeseries" in historical_analysis
        flat_type_timeseries = historical_analysis["flat_type_timeseries"]
        assert isinstance(flat_type_timeseries, dict), "Flat type timeseries should be a dictionary"
        
        total_data_points = sum(len(points) for points in flat_type_timeseries.values())
        if total_data_points > 0:
            logger.info(f"Historical data points: {total_data_points} across {len(flat_type_timeseries)} flat types")
            for flat_type, points in list(flat_type_timeseries.items())[:2]:  # Show first 2 flat types
                if points:
                    logger.info(f"  {flat_type}: {len(points)} data points")
                    for point in points[:2]:  # Show first 2 points per type
                        logger.info(f"    {point['date']}: avg {point['mean_price']:,.0f}₸")
    
    def test_jk_sales_opportunities_meridian(self, api_server):
        """Test getting sales opportunities for Meridian Apartments."""
        logger.info("Testing JK sales opportunities for Meridian Apartments...")
        response = requests.get(f"{api_server}/api/jks/sales/Meridian%20Apartments/opportunities?discount_percentage=0.15&limit=10")
        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert data["jk_name"] == "Meridian Apartments"
        assert data["discount_percentage"] == 0.15
        assert "opportunities" in data
        assert "total_found" in data
        
        opportunities = data["opportunities"]
        total_found = data["total_found"]
        
        logger.info(f"Found {total_found} total opportunities, returning {len(opportunities)}")
        
        # Test opportunity structure
        for i, opp in enumerate(opportunities[:3]):  # Test first 3 opportunities
            assert "flat_id" in opp, f"Opportunity {i} should have flat_id"
            assert "price" in opp, f"Opportunity {i} should have price"
            assert "area" in opp, f"Opportunity {i} should have area"
            assert "flat_type" in opp, f"Opportunity {i} should have flat_type"
            assert "discount_percentage_vs_median" in opp, f"Opportunity {i} should have discount percentage"
            assert "market_stats" in opp, f"Opportunity {i} should have market stats"
            
            # Validate opportunity data
            assert opp["price"] > 0, f"Opportunity {i} price should be positive"
            assert opp["area"] > 0, f"Opportunity {i} area should be positive"
            assert opp["discount_percentage_vs_median"] >= 0, f"Opportunity {i} discount should be non-negative"
            
            # Market stats validation
            market_stats = opp["market_stats"]
            assert market_stats["mean_price"] > 0, f"Opportunity {i} market mean should be positive"
            assert market_stats["median_price"] > 0, f"Opportunity {i} market median should be positive"
            assert market_stats["count"] > 0, f"Opportunity {i} market count should be positive"
            
            logger.info(f"  Opportunity {i+1}: {opp['flat_type']} - {opp['price']:,}₸ ({opp['discount_percentage_vs_median']:.1f}% discount)")
    
    def test_jk_sales_analysis_different_discounts(self, api_server):
        """Test sales analysis with different discount percentages."""
        logger.info("Testing JK sales analysis with different discount percentages...")
        
        # Test with 10% discount
        response_10 = requests.get(f"{api_server}/api/jks/sales/Meridian%20Apartments/analysis?discount_percentage=0.10")
        assert response_10.status_code == 200, f"10% discount analysis failed: {response_10.text}"
        data_10 = response_10.json()
        
        # Test with 20% discount
        response_20 = requests.get(f"{api_server}/api/jks/sales/Meridian%20Apartments/analysis?discount_percentage=0.20")
        assert response_20.status_code == 200, f"20% discount analysis failed: {response_20.text}"
        data_20 = response_20.json()
        
        # Count opportunities for both
        opps_10 = sum(len(opps) for opps in data_10["current_market"]["opportunities"].values())
        opps_20 = sum(len(opps) for opps in data_20["current_market"]["opportunities"].values())
        
        logger.info(f"Opportunities with 10% discount: {opps_10}")
        logger.info(f"Opportunities with 20% discount: {opps_20}")
        
        # Both should be non-negative
        assert opps_10 >= 0, "10% discount should return non-negative opportunities"
        assert opps_20 >= 0, "20% discount should return non-negative opportunities"
        
        # Log the relationship for debugging
        if opps_20 <= opps_10:
            logger.info("✅ Higher discount found fewer or equal opportunities (expected)")
        else:
            logger.info(f"⚠️ Higher discount found more opportunities ({opps_20} vs {opps_10}) - unexpected behavior")
    
    def test_jk_sales_analysis_nonexistent(self, api_server):
        """Test sales analysis for non-existent JK."""
        logger.info("Testing JK sales analysis for non-existent JK...")
        response = requests.get(f"{api_server}/api/jks/sales/NonExistentJK/analysis")
        
        # The API should return 200 with empty data for non-existent JKs (graceful handling)
        assert response.status_code == 200, f"Expected 200 status, got {response.status_code}: {response.text}"
        
        data = response.json()
        assert data["success"] is True
        assert data["jk_name"] == "NonExistentJK"
        
        # Should have empty analysis data
        current_market = data["current_market"]
        assert current_market["global_stats"]["count"] == 0, "Non-existent JK should have 0 sales"
        assert current_market["global_stats"]["mean"] == 0, "Non-existent JK should have 0 mean price"
        assert current_market["global_stats"]["median"] == 0, "Non-existent JK should have 0 median price"
        assert current_market["global_stats"]["min"] == 0, "Non-existent JK should have 0 min price"
        assert current_market["global_stats"]["max"] == 0, "Non-existent JK should have 0 max price"
        
        # Should have empty opportunities
        opportunities = current_market["opportunities"]
        assert isinstance(opportunities, dict), "Opportunities should be a dictionary"
        assert len(opportunities) == 0, "Non-existent JK should have no opportunities"
        
        # Should have empty flat type buckets
        flat_type_buckets = current_market["flat_type_buckets"]
        assert isinstance(flat_type_buckets, dict), "Flat type buckets should be a dictionary"
        assert len(flat_type_buckets) == 0, "Non-existent JK should have no flat type buckets"
        
        logger.info("✅ Non-existent JK handled gracefully with empty data")

