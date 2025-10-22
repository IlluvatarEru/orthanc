"""
Tests for the flats API endpoints.
"""
import logging
import subprocess
import sys
import time

import pytest
import requests

logger = logging.getLogger(__name__)


class TestFlatsAPI:
    """Test class for flats API endpoints."""

    @pytest.fixture(scope="class")
    def api_server(self):
        """Start API server for testing."""
        # Start the API server in a subprocess
        process = subprocess.Popen([
            sys.executable, "-m", "api.launch.launch_api",
            "--host", "127.0.0.1",
            "--port", "8002"
        ], stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        # Wait for server to start
        time.sleep(3)

        yield "http://127.0.0.1:8002"

        # Clean up
        process.terminate()
        process.wait()

    def test_flats_search_basic(self, api_server):
        """Test basic flats search without filters."""
        logger.info("Testing basic flats search...")
        response = requests.get(f"{api_server}/api/flats/search", params={"limit": 10})
        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert "flats" in data
        logger.info(f"Found {data['count']} flats")

        # Log some sample data for debugging
        if data['flats']:
            sample_flat = data['flats'][0]
            logger.info(f"Sample flat: {sample_flat}")

    def test_flats_search_by_flat_type(self, api_server):
        """Test flats search by flat type."""
        logger.info("Testing flats search by flat type...")
        response = requests.get(f"{api_server}/api/flats/search", params={
            "flat_type": "2BR",
            "limit": 10
        })
        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        logger.info(f"Found {data['count']} flats with flat_type=2BR")

        # Verify all returned flats have the correct flat_type
        for flat in data['flats']:
            assert flat['flat_type'] == '2BR', f"Expected flat_type=2BR, got {flat['flat_type']}"

    def test_flats_search_by_residential_complex(self, api_server):
        """Test flats search by residential complex."""
        logger.info("Testing flats search by residential complex...")
        response = requests.get(f"{api_server}/api/flats/search", params={
            "residential_complex": "Meridian Apartments",
            "limit": 10
        })
        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        logger.info(f"Found {data['count']} flats in Meridian Apartments")
        assert data['count'] == 10

        # Log the actual residential_complex values for debugging
        for i, flat in enumerate(data['flats'][:3]):  # Show first 3
            assert flat.get('residential_complex') == "Meridian Apartments"

    def test_flats_search_combined_filters(self, api_server):
        """Test flats search with combined filters (the problematic case)."""
        logger.info("Testing flats search with combined filters...")
        response = requests.get(f"{api_server}/api/flats/search", params={
            "flat_type": "2BR",
            "residential_complex": "Meridian Apartments",
            "limit": 10
        })
        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        logger.info(f"Found {data['count']} flats with flat_type=2BR AND residential_complex='Meridian Apartments'")

        # If no results, let's debug by checking what's in the database
        if data['count'] == 0:
            logger.warning("No results found with combined filters. Debugging...")

            # Check what residential complexes exist
            response_all = requests.get(f"{api_server}/api/flats/search", params={"limit": 50})
            if response_all.status_code == 200:
                all_data = response_all.json()
                residential_complexes = set()
                flat_types = set()

                for flat in all_data['flats']:
                    if flat.get('residential_complex'):
                        residential_complexes.add(flat['residential_complex'])
                    if flat.get('flat_type'):
                        flat_types.add(flat['flat_type'])

                logger.info(f"Available residential complexes: {sorted(residential_complexes)}")
                logger.info(f"Available flat types: {sorted(flat_types)}")

                # Check for Meridian Apartments specifically
                meridian_flats = [f for f in all_data['flats'] if 'Meridian' in f.get('residential_complex', '')]
                logger.info(f"Flats with 'Meridian' in residential_complex: {len(meridian_flats)}")
                for flat in meridian_flats[:3]:
                    logger.info(
                        f"  - {flat.get('residential_complex')} | {flat.get('flat_type')} | {flat.get('flat_id')}")

    def test_flats_search_price_filters(self, api_server):
        """Test flats search with price filters."""
        logger.info("Testing flats search with price filters...")
        response = requests.get(f"{api_server}/api/flats/search", params={
            "min_price": 10000000,
            "max_price": 100000000,
            "limit": 10
        })
        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        logger.info(f"Found {data['count']} flats with price between 10M and 100M")

        # Verify price constraints
        for flat in data['flats']:
            assert flat['price'] >= 10000000, f"Price {flat['price']} below min_price"
            assert flat['price'] <= 100000000, f"Price {flat['price']} above max_price"

    def test_flats_search_rental_filter(self, api_server):
        """Test flats search with rental filter."""
        logger.info("Testing flats search with rental filter...")

        # Test rental flats
        response_rental = requests.get(f"{api_server}/api/flats/search", params={
            "is_rental": True,
            "limit": 10
        })
        assert response_rental.status_code == 200
        data_rental = response_rental.json()
        logger.info(f"Found {data_rental['count']} rental flats")

        # Test sales flats
        response_sales = requests.get(f"{api_server}/api/flats/search", params={
            "is_rental": False,
            "limit": 10
        })
        assert response_sales.status_code == 200
        data_sales = response_sales.json()
        logger.info(f"Found {data_sales['count']} sales flats")

    def test_flats_summary(self, api_server):
        """Test flats summary statistics."""
        logger.info("Testing flats summary...")
        response = requests.get(f"{api_server}/api/flats/stats/summary")
        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert "rentals" in data
        assert "sales" in data

        logger.info(f"Rental stats: {data['rentals']}")
        logger.info(f"Sales stats: {data['sales']}")

    def test_flats_search_debug_all_data(self, api_server):
        """Debug test to see what data is actually in the database."""
        logger.info("Debugging: Getting all flats data...")
        response = requests.get(f"{api_server}/api/flats/search", params={"limit": 100})
        assert response.status_code == 200
        data = response.json()

        logger.info(f"Total flats found: {data['count']}")

        if data['flats']:
            # Analyze the data
            residential_complexes = {}
            flat_types = {}

            for flat in data['flats']:
                rc = flat.get('residential_complex', 'NULL')
                ft = flat.get('flat_type', 'NULL')

                residential_complexes[rc] = residential_complexes.get(rc, 0) + 1
                flat_types[ft] = flat_types.get(ft, 0) + 1

            logger.info("Residential complexes distribution:")
            for rc, count in sorted(residential_complexes.items()):
                logger.info(f"  {rc}: {count}")

            logger.info("Flat types distribution:")
            for ft, count in sorted(flat_types.items()):
                logger.info(f"  {ft}: {count}")

            # Look for Meridian specifically
            meridian_flats = [f for f in data['flats'] if
                              f.get('residential_complex') and 'Meridian' in f['residential_complex']]
            logger.info(f"Flats with 'Meridian' in residential_complex: {len(meridian_flats)}")

            for flat in meridian_flats[:5]:  # Show first 5
                logger.info(
                    f"  - ID: {flat.get('flat_id')}, Type: {flat.get('flat_type')}, Price: {flat.get('price')}, RC: '{flat.get('residential_complex')}'")
