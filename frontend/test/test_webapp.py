#!/usr/bin/env python3
"""
Test script for the webapp frontend.

These tests require the webapp and API server to be running.
Run with: pytest frontend/test/test_webapp.py -v -s

Note: These are integration tests that require external services.
They are marked with pytest.mark.integration to allow skipping in CI.
"""

import logging
import pytest
import requests

logger = logging.getLogger(__name__)


@pytest.mark.integration
class TestWebappConnectivity:
    """Test class for webapp connectivity tests."""

    @pytest.fixture
    def webapp_url(self):
        """Get webapp base URL."""
        return "http://localhost:5000"

    @pytest.fixture
    def api_url(self):
        """Get API base URL."""
        return "http://localhost:8000"

    def test_webapp_connectivity(self, webapp_url):
        """Test if webapp is running and responding."""
        try:
            response = requests.get(f"{webapp_url}/", timeout=5)
            assert response.status_code == 200, (
                f"Webapp returned status {response.status_code}"
            )
            logger.info("Webapp is running and responding")
        except requests.exceptions.ConnectionError:
            pytest.skip("Webapp is not running on localhost:5000")

    def test_api_connectivity(self, api_url):
        """Test if API server is running."""
        try:
            response = requests.get(f"{api_url}/api/health", timeout=5)
            assert response.status_code == 200, (
                f"API server returned status {response.status_code}"
            )
            logger.info("API server is running and responding")
        except requests.exceptions.ConnectionError:
            pytest.skip("API server is not running on localhost:8000")

    def test_analyze_jk_endpoint(self, webapp_url):
        """Test the analyze_jk endpoint."""
        try:
            response = requests.get(
                f"{webapp_url}/analyze_jk/Meridian%20Apartments", timeout=10
            )
            assert response.status_code == 200, (
                f"Analyze JK endpoint returned status {response.status_code}"
            )
            logger.info("Analyze JK endpoint is working")
        except requests.exceptions.ConnectionError:
            pytest.skip("Webapp is not running on localhost:5000")
