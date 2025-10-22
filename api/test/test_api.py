"""
Tests for the Orthanc Real Estate Analytics API.
"""
import logging
import subprocess
import sys
import time

import pytest
import requests

logger = logging.getLogger(__name__)


class TestAPI:
    """Test class for API endpoints."""

    @pytest.fixture(scope="class")
    def api_server(self):
        """Start API server for testing."""
        # Start the API server in a subprocess
        process = subprocess.Popen([
            sys.executable, "-m", "api.launch.launch_api",
            "--host", "127.0.0.1",
            "--port", "8001"
        ], stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        # Wait for server to start
        time.sleep(3)

        yield "http://127.0.0.1:8001"

        # Clean up
        process.terminate()
        process.wait()

    def test_root_endpoint(self, api_server):
        """Test root endpoint."""
        response = requests.get(f"{api_server}/")
        assert response.status_code == 200
        data = response.json()
        assert "message" in data
        assert "version" in data

    def test_health_check(self, api_server):
        """Test health check endpoint."""
        response = requests.get(f"{api_server}/api/health")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"

    def test_jk_sales_list(self, api_server):
        """Test JK sales list endpoint."""
        response = requests.get(f"{api_server}/api/jks/sales/")
        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert "jks" in data

    def test_jk_rentals_list(self, api_server):
        """Test JK rentals list endpoint."""
        response = requests.get(f"{api_server}/api/jks/rentals/")
        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert "jks" in data

    def test_flats_search(self, api_server):
        """Test flats search endpoint."""
        response = requests.get(f"{api_server}/api/flats/search", params={"limit": 10})
        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert "flats" in data

    def test_flats_summary(self, api_server):
        """Test flats summary endpoint."""
        response = requests.get(f"{api_server}/api/flats/stats/summary")
        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert "rentals" in data
        assert "sales" in data

    def test_rentals_overview(self, api_server):
        """Test rentals overview endpoint."""
        response = requests.get(f"{api_server}/api/jks/rentals/stats/overview")
        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert "overview" in data
