"""
Tests for the hot JKs API endpoints.

pytest api/test/test_hot_jks_api.py -v
"""

import subprocess
import sys
import time

import pytest
import requests


class TestHotJKsAPI:
    @pytest.fixture(scope="class")
    def api_server(self):
        process = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "api.launch.launch_api",
                "--host",
                "127.0.0.1",
                "--port",
                "8099",
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        time.sleep(3)
        yield "http://127.0.0.1:8099"
        process.terminate()
        process.wait()

    def test_hot_jks_endpoint_status(self, api_server):
        response = requests.get(f"{api_server}/api/jks/hot")
        assert response.status_code == 200

    def test_hot_jks_response_structure(self, api_server):
        response = requests.get(f"{api_server}/api/jks/hot")
        data = response.json()
        assert "week" in data
        assert "rankings" in data
        assert isinstance(data["rankings"], list)

    def test_hot_jks_rankings_sorted(self, api_server):
        response = requests.get(f"{api_server}/api/jks/hot")
        data = response.json()
        rankings = data["rankings"]
        if len(rankings) >= 2:
            for i in range(len(rankings) - 1):
                assert rankings[i]["heat_score"] >= rankings[i + 1]["heat_score"]

    def test_hot_jks_score_range(self, api_server):
        response = requests.get(f"{api_server}/api/jks/hot")
        data = response.json()
        for r in data["rankings"]:
            assert 0 <= r["heat_score"] <= 1

    def test_hot_jks_limit_param(self, api_server):
        response = requests.get(f"{api_server}/api/jks/hot", params={"limit": 5})
        data = response.json()
        assert len(data["rankings"]) <= 5

    def test_price_trends_endpoint_status(self, api_server):
        response = requests.get(f"{api_server}/api/jks/price-trends")
        assert response.status_code == 200

    def test_price_trends_response_structure(self, api_server):
        response = requests.get(f"{api_server}/api/jks/price-trends")
        data = response.json()
        assert "week" in data
        assert "jks" in data
        assert isinstance(data["jks"], list)
