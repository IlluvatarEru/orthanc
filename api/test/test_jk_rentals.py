"""
Tests for JK rentals analysis API endpoints.
"""
import pytest
import requests
import time
import subprocess
import logging

logger = logging.getLogger(__name__)

class TestJKRentalsAPI:
    """Test class for JK rentals analysis endpoints."""
    
    @pytest.fixture(scope="class")
    def api_server(self):
        """Start API server for testing."""
        # Start the API server in a subprocess
        process = subprocess.Popen([
            "python", "-m", "api.launch.launch_api", 
            "--host", "127.0.0.1", 
            "--port", "8005"
        ], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        
        # Wait for server to start
        time.sleep(5)
        
        yield "http://127.0.0.1:8005"
        
        # Clean up
        process.terminate()
        process.wait()
    
    def test_jk_rentals_list(self, api_server):
        """Test getting list of JKs with rental data."""
        logger.info("Testing JK rentals list...")
        response = requests.get(f"{api_server}/api/jks/rentals/")
        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert "jks" in data
        logger.info(f"Found {len(data['jks'])} JKs with rental data")
        
        # Should have at least some JKs with rental data
        assert len(data["jks"]) > 0, "Should have at least one JK with rental data"
        
        # Log first few JKs
        for jk in data["jks"][:5]:  # Show first 5
            logger.info(f"  {jk}")
    
    def test_jk_rentals_summary_meridian(self, api_server):
        """Test getting rental summary for Meridian Apartments."""
        logger.info("Testing JK rentals summary for Meridian Apartments...")
        response = requests.get(f"{api_server}/api/jks/rentals/Meridian%20Apartments/summary")
        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert data["jk_name"] == "Meridian Apartments"
        assert "summary" in data
        
        summary = data["summary"]
        logger.info(f"Meridian Apartments rental summary: {summary}")
        
        # Test summary structure
        assert "total_rentals" in summary, "Summary should have total_rentals"
        assert "date_range" in summary, "Summary should have date_range"
        assert "flat_type_distribution" in summary, "Summary should have flat_type_distribution"
        
        # Test total rentals
        assert summary["total_rentals"] > 0, "Meridian Apartments should have rental data"
        
        # Test date range
        date_range = summary["date_range"]
        assert "earliest" in date_range, "Date range should have earliest"
        assert "latest" in date_range, "Date range should have latest"
        
        # Test flat type distribution
        flat_type_dist = summary["flat_type_distribution"]
        assert len(flat_type_dist) > 0, "Should have at least one flat type"
        
        for flat_type, count in flat_type_dist.items():
            assert count > 0, f"{flat_type} should have rental data"
            logger.info(f"  {flat_type}: {count} rentals")
        
        logger.info(f"✅ Meridian Apartments has {summary['total_rentals']} rentals")
    
    def test_jk_rentals_analysis_meridian(self, api_server):
        """Test getting rental analysis for Meridian Apartments."""
        logger.info("Testing JK rentals analysis for Meridian Apartments...")
        response = requests.get(f"{api_server}/api/jks/rentals/Meridian%20Apartments/analysis?min_yield_percentage=0.05")
        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert data["jk_name"] == "Meridian Apartments"
        assert data["min_yield_percentage"] == 0.05
        assert "current_market" in data
        assert "historical_analysis" in data
        
        # Test current market analysis
        current_market = data["current_market"]
        assert "global_stats" in current_market
        assert "flat_type_buckets" in current_market
        assert "opportunities" in current_market
        
        # Test global stats
        global_stats = current_market["global_stats"]
        assert "mean_yield" in global_stats
        assert "median_yield" in global_stats
        assert "min_yield" in global_stats
        assert "max_yield" in global_stats
        assert "count" in global_stats
        
        logger.info(f"Global rental yield stats: {global_stats}")
        
        # Test flat type buckets
        flat_type_buckets = current_market["flat_type_buckets"]
        logger.info(f"Flat type buckets: {list(flat_type_buckets.keys())}")
        
        for flat_type, stats in flat_type_buckets.items():
            assert "mean_yield" in stats
            assert "median_yield" in stats
            assert "min_yield" in stats
            assert "max_yield" in stats
            assert "count" in stats
            assert stats["count"] > 0, f"{flat_type} should have rental data"
            logger.info(f"  {flat_type}: {stats['count']} rentals, mean yield {stats['mean_yield']:.2f}%")
        
        # Test opportunities
        opportunities = current_market["opportunities"]
        total_opportunities = sum(len(opps) for opps in opportunities.values())
        logger.info(f"Total opportunities found: {total_opportunities}")
        
        for flat_type, opps in opportunities.items():
            if opps:
                logger.info(f"  {flat_type}: {len(opps)} opportunities")
                for i, opp in enumerate(opps[:2]):  # Show first 2
                    assert "flat_id" in opp
                    assert "price" in opp
                    assert "yield_percentage" in opp
                    assert opp["yield_percentage"] >= 0.05, f"Opportunity yield {opp['yield_percentage']}% should be >= 5%"
                    logger.info(f"    {i+1}. Flat {opp['flat_id']}: {opp['price']:,}₸/month (Yield: {opp['yield_percentage']:.2f}%)")
        
        # Test historical analysis
        historical = data["historical_analysis"]
        assert "flat_type_timeseries" in historical
        
        timeseries = historical["flat_type_timeseries"]
        total_data_points = sum(len(points) for points in timeseries.values())
        logger.info(f"Historical data points: {total_data_points}")
        
        for flat_type, points in timeseries.items():
            if points:
                logger.info(f"  {flat_type}: {len(points)} historical points")
                for point in points[:2]:  # Show first 2
                    assert "date" in point
                    assert "mean_rental" in point
                    assert "median_rental" in point
                    assert point["mean_rental"] > 0, "Mean rental should be positive"
                    logger.info(f"    {point['date']}: {point['count']} rentals, avg {point['mean_rental']:,.0f}₸")
        
        logger.info(f"✅ Meridian Apartments rental analysis completed with {total_opportunities} opportunities")
    
    def test_jk_rentals_opportunities_meridian(self, api_server):
        """Test getting rental opportunities for Meridian Apartments."""
        logger.info("Testing JK rental opportunities for Meridian Apartments...")
        response = requests.get(f"{api_server}/api/jks/rentals/Meridian%20Apartments/opportunities?min_yield_percentage=0.05&limit=5")
        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert data["jk_name"] == "Meridian Apartments"
        assert data["min_yield_percentage"] == 0.05
        assert "opportunities" in data
        assert "count" in data
        assert "total_found" in data
        
        opportunities = data["opportunities"]
        count = data["count"]
        total_found = data["total_found"]
        
        logger.info(f"Found {count} opportunities (total: {total_found})")
        
        # Should have at least some opportunities or gracefully handle none
        assert count >= 0, "Opportunities count should be non-negative"
        assert count <= 5, "Should respect limit of 5"
        assert count <= total_found, "Count should not exceed total found"
        
        # Test opportunity structure
        for i, opp in enumerate(opportunities):
            assert "flat_id" in opp, f"Opportunity {i} should have flat_id"
            assert "price" in opp, f"Opportunity {i} should have price"
            assert "yield_percentage" in opp, f"Opportunity {i} should have yield_percentage"
            assert "flat_type" in opp, f"Opportunity {i} should have flat_type"
            assert "residential_complex" in opp, f"Opportunity {i} should have residential_complex"
            
            # Validate opportunity data
            assert opp["price"] > 0, f"Opportunity {i} price should be positive"
            assert opp["yield_percentage"] >= 0.05, f"Opportunity {i} yield {opp['yield_percentage']}% should be >= 5%"
            assert opp["residential_complex"] == "Meridian Apartments", f"Opportunity {i} should be from Meridian Apartments"
            
            logger.info(f"  Opportunity {i+1}: {opp['flat_type']} - {opp['price']:,}₸/month (Yield: {opp['yield_percentage']:.2f}%)")
        
        logger.info(f"✅ Found {count} rental opportunities for Meridian Apartments")
    
    def test_jk_rentals_listings_meridian(self, api_server):
        """Test getting rental listings for Meridian Apartments."""
        logger.info("Testing JK rental listings for Meridian Apartments...")
        response = requests.get(f"{api_server}/api/jks/rentals/Meridian%20Apartments/rentals?limit=20")
        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert data["jk_name"] == "Meridian Apartments"
        assert "rentals" in data
        assert "count" in data
        
        rentals = data["rentals"]
        count = data["count"]
        
        logger.info(f"Found {count} rental listings for Meridian Apartments")
        
        # Should have at least some rentals
        assert count > 0, "Meridian Apartments should have rental listings"
        assert len(rentals) > 0, "Should return rental data"
        
        # Test rental structure
        for i, rental in enumerate(rentals[:5]):  # Test first 5 rentals
            assert "flat_id" in rental, f"Rental {i} should have flat_id"
            assert "price" in rental, f"Rental {i} should have price"
            assert "area" in rental, f"Rental {i} should have area"
            assert "flat_type" in rental, f"Rental {i} should have flat_type"
            assert "residential_complex" in rental, f"Rental {i} should have residential_complex"
            
            # Validate rental data
            assert rental["price"] > 0, f"Rental {i} price should be positive"
            assert rental["area"] > 0, f"Rental {i} area should be positive"
            assert rental["residential_complex"] == "Meridian Apartments", f"Rental {i} should be from Meridian Apartments"
            
            # Price should be reasonable for rentals
            assert 50_000 <= rental["price"] <= 2_000_000, f"Rental {i} price {rental['price']} should be reasonable for Almaty rentals"
            
            logger.info(f"  Rental {i+1}: {rental['flat_type']} - {rental['price']:,}₸/month, {rental['area']}m²")
    
    def test_jk_rentals_with_filters(self, api_server):
        """Test rental listings with various filters."""
        logger.info("Testing JK rental listings with filters...")
        
        # Test with flat_type filter
        response_type = requests.get(f"{api_server}/api/jks/rentals/Meridian%20Apartments/rentals?flat_type=2BR&limit=10")
        assert response_type.status_code == 200
        data_type = response_type.json()
        logger.info(f"2BR rentals: {data_type['count']}")
        
        # Test with price filters
        response_price = requests.get(f"{api_server}/api/jks/rentals/Meridian%20Apartments/rentals?min_price=100000&max_price=500000&limit=10")
        assert response_price.status_code == 200
        data_price = response_price.json()
        logger.info(f"Rentals 100k-500k: {data_price['count']}")
        
        # Validate filtered results
        for rental in data_price["rentals"]:
            assert 100_000 <= rental["price"] <= 500_000, f"Rental price {rental['price']} should be within filter range"
    
    def test_jk_rentals_price_trends_meridian(self, api_server):
        """Test getting rental price trends for Meridian Apartments."""
        logger.info("Testing JK rental price trends for Meridian Apartments...")
        response = requests.get(f"{api_server}/api/jks/rentals/Meridian%20Apartments/price-trends")
        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert data["jk_name"] == "Meridian Apartments"
        assert "price_trends" in data
        
        trends = data["price_trends"]
        logger.info(f"Found {len(trends)} price trend data points")
        
        if trends:
            # Test trend data structure
            for i, trend in enumerate(trends[:3]):  # Test first 3 trends
                assert "date" in trend, f"Trend {i} should have date"
                assert "flat_type" in trend, f"Trend {i} should have flat_type"
                assert "count" in trend, f"Trend {i} should have count"
                assert "avg_price" in trend, f"Trend {i} should have avg_price"
                assert "min_price" in trend, f"Trend {i} should have min_price"
                assert "max_price" in trend, f"Trend {i} should have max_price"
                
                # Validate trend data
                assert trend["count"] > 0, f"Trend {i} count should be positive"
                assert trend["avg_price"] > 0, f"Trend {i} avg_price should be positive"
                assert trend["min_price"] > 0, f"Trend {i} min_price should be positive"
                assert trend["max_price"] > 0, f"Trend {i} max_price should be positive"
                
                # Price logic validation
                assert trend["min_price"] <= trend["avg_price"] <= trend["max_price"], f"Trend {i}: Min <= Avg <= Max should hold"
                
                logger.info(f"  {trend['date']} - {trend['flat_type']}: {trend['count']} rentals, avg {trend['avg_price']:,.0f}₸")
        else:
            logger.info("No price trend data available (this might be normal if data is recent)")
    
    def test_rentals_overview(self, api_server):
        """Test overall rental market overview."""
        logger.info("Testing rentals overview...")
        response = requests.get(f"{api_server}/api/jks/rentals/stats/overview")
        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert "overview" in data
        
        overview = data["overview"]
        assert "overall_stats" in overview
        assert "top_jks" in overview
        assert "flat_type_distribution" in overview
        
        # Test overall stats
        overall_stats = overview["overall_stats"]
        assert overall_stats["total_rentals"] > 0, "Should have rental data"
        assert overall_stats["total_jks"] > 0, "Should have JKs with rental data"
        assert overall_stats["avg_price"] > 0, "Average rental price should be positive"
        assert overall_stats["min_price"] > 0, "Minimum rental price should be positive"
        assert overall_stats["max_price"] > 0, "Maximum rental price should be positive"
        assert overall_stats["avg_area"] > 0, "Average area should be positive"
        
        # Test top JKs
        top_jks = overview["top_jks"]
        assert len(top_jks) > 0, "Should have top JKs"
        for jk in top_jks[:3]:  # Test first 3
            assert "residential_complex" in jk, "JK should have name"
            assert "rental_count" in jk, "JK should have rental count"
            assert "avg_price" in jk, "JK should have avg price"
            assert jk["rental_count"] > 0, "JK should have rentals"
            assert jk["avg_price"] > 0, "JK avg price should be positive"
            logger.info(f"  {jk['residential_complex']}: {jk['rental_count']} rentals, avg {jk['avg_price']:,.0f}₸")
        
        # Test flat type distribution
        flat_type_dist = overview["flat_type_distribution"]
        assert len(flat_type_dist) > 0, "Should have flat type distribution"
        for dist in flat_type_dist:
            assert "flat_type" in dist, "Distribution should have flat_type"
            assert "count" in dist, "Distribution should have count"
            assert "avg_price" in dist, "Distribution should have avg_price"
            assert dist["count"] > 0, "Flat type should have rentals"
            assert dist["avg_price"] > 0, "Flat type avg price should be positive"
            logger.info(f"  {dist['flat_type']}: {dist['count']} rentals, avg {dist['avg_price']:,.0f}₸")
        
        logger.info(f"✅ Rental market overview: {overall_stats['total_rentals']} rentals across {overall_stats['total_jks']} JKs")
    
    def test_jk_rentals_nonexistent(self, api_server):
        """Test rental analysis for non-existent JK."""
        logger.info("Testing JK rentals analysis for non-existent JK...")
        response = requests.get(f"{api_server}/api/jks/rentals/NonExistentJK/summary")
        # Should return 200 with empty data for non-existent JK
        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert data["jk_name"] == "NonExistentJK"
        assert data["summary"]["total_rentals"] == 0, "Non-existent JK should have 0 rentals"
    
    def test_jk_rentals_analysis_nonexistent(self, api_server):
        """Test rental analysis for non-existent JK."""
        logger.info("Testing JK rentals analysis for non-existent JK...")
        response = requests.get(f"{api_server}/api/jks/rentals/NonExistentJK/analysis")
        # Should return 200 with empty data for non-existent JK
        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert data["jk_name"] == "NonExistentJK"
        assert data["current_market"]["global_stats"]["count"] == 0, "Non-existent JK should have 0 rental data"
        assert len(data["current_market"]["opportunities"]) == 0, "Non-existent JK should have no opportunities"

if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s", "--log-cli-level=INFO"])
