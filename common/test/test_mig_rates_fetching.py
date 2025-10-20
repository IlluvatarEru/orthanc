"""
Test script for fetching mid prices from mig.kz for EUR, USD, and GBP vs KZT.
"""

import sys
import os
from datetime import datetime
import logging
import pytest

from common.src.currency import CurrencyManager

# Rate bounds for validation (KZT per currency unit)
LOWER_BOUND_EUR_KZT = 400
UPPER_BOUND_EUR_KZT = 700
LOWER_BOUND_USD_KZT = 400
UPPER_BOUND_USD_KZT = 700


class TestMigRatesFetching:
    """Test class for MIG rates fetching functionality."""

    @pytest.fixture
    def currency_manager(self):
        """Create currency manager instance."""
        return CurrencyManager()

    @pytest.fixture
    def test_currencies(self):
        """Test currencies to fetch."""
        return ['EUR', 'USD']

    def test_fetch_eur_rate(self, currency_manager):
        """Test fetching EUR rate from mig.kz."""
        rate = currency_manager.get_latest_rate('EUR')

        assert rate is not None, "EUR rate should not be None"
        assert rate > LOWER_BOUND_EUR_KZT, f"EUR rate should be above {LOWER_BOUND_EUR_KZT}, got {rate}"
        assert rate < UPPER_BOUND_EUR_KZT, f"EUR rate should be below {UPPER_BOUND_EUR_KZT}, got {rate}"

    def test_fetch_usd_rate(self, currency_manager):
        """Test fetching USD rate from mig.kz."""
        rate = currency_manager.get_latest_rate('USD')

        assert rate is not None, "USD rate should not be None"
        assert rate > LOWER_BOUND_USD_KZT, f"USD rate should be above {LOWER_BOUND_USD_KZT}, got {rate}"
        assert rate < UPPER_BOUND_USD_KZT, f"USD rate should be below {UPPER_BOUND_USD_KZT}, got {rate}"

    def test_fetch_multiple_rates(self, currency_manager, test_currencies):
        """Test fetching multiple currency rates."""
        fetched_rates = {}

        for currency in test_currencies:
            rate = currency_manager.get_latest_rate(currency)
            fetched_rates[currency] = rate

            assert rate is not None, f"{currency} rate should not be None"

            # Use appropriate bounds for each currency
            if currency == 'EUR':
                assert rate > LOWER_BOUND_EUR_KZT, f"EUR rate should be above {LOWER_BOUND_EUR_KZT}, got {rate}"
                assert rate < UPPER_BOUND_EUR_KZT, f"EUR rate should be below {UPPER_BOUND_EUR_KZT}, got {rate}"
            elif currency == 'USD':
                assert rate > LOWER_BOUND_USD_KZT, f"USD rate should be above {LOWER_BOUND_USD_KZT}, got {rate}"
                assert rate < UPPER_BOUND_USD_KZT, f"USD rate should be below {UPPER_BOUND_USD_KZT}, got {rate}"

        assert len(fetched_rates) == len(test_currencies), \
            f"Expected {len(test_currencies)} rates, got {len(fetched_rates)}"

    def test_convert_kzt_to_eur(self, currency_manager):
        """Test converting KZT to EUR."""
        test_amount = 1e6
        converted = currency_manager.convert_kzt_to_eur(test_amount)

        assert converted is not None, "EUR conversion should not be None"
        assert converted > test_amount / UPPER_BOUND_EUR_KZT, f"Converted EUR amount should be positive, got {converted}"
        assert converted < test_amount / LOWER_BOUND_EUR_KZT, f"Converted amount should be less than original, got {converted}"

    def test_convert_kzt_to_usd(self, currency_manager):
        """Test converting KZT to USD."""
        test_amount = 1e6
        converted = currency_manager.convert_kzt_to_usd(test_amount)

        assert converted is not None, "USD conversion should not be None"
        assert converted > test_amount / UPPER_BOUND_USD_KZT, f"Converted USD amount should be positive, got {converted}"
        assert converted < test_amount / LOWER_BOUND_USD_KZT, f"Converted amount should be less than original, got {converted}"

    def test_price_formatting(self, currency_manager):
        """Test price formatting with EUR conversion."""
        from common.src.currency import format_price_with_eur

        test_amount = 1e6
        formatted = format_price_with_eur(test_amount, currency_manager, show_eur=True)

        assert formatted is not None, "Formatted price should not be None"
        assert "₸" in formatted, f"Formatted price should contain ₸, got {formatted}"
        assert "€" in formatted, f"Formatted price should contain €, got {formatted}"

    def test_rate_validation(self, currency_manager):
        """Test that fetched rates are within reasonable ranges."""
        currencies = ['EUR', 'USD']

        for currency in currencies:
            rate = currency_manager.get_latest_rate(currency)

            # Basic validation
            assert rate is not None, f"{currency} rate should not be None"
            assert isinstance(rate, (int, float)), f"{currency} rate should be numeric, got {type(rate)}"
            assert rate > 0, f"{currency} rate should be positive, got {rate}"

            # Use defined bounds for validation
            if currency == 'EUR':
                assert rate > LOWER_BOUND_EUR_KZT, f"EUR rate should be above {LOWER_BOUND_EUR_KZT}, got {rate}"
                assert rate < UPPER_BOUND_EUR_KZT, f"EUR rate should be below {UPPER_BOUND_EUR_KZT}, got {rate}"
            elif currency == 'USD':
                assert rate > LOWER_BOUND_USD_KZT, f"USD rate should be above {LOWER_BOUND_USD_KZT}, got {rate}"
                assert rate < UPPER_BOUND_USD_KZT, f"USD rate should be below {UPPER_BOUND_USD_KZT}, got {rate}"

    def test_currency_manager_initialization(self):
        """Test that CurrencyManager initializes correctly."""
        manager = CurrencyManager()

        assert manager is not None, "CurrencyManager should initialize"
        assert hasattr(manager, 'get_latest_rate'), "CurrencyManager should have get_latest_rate method"
        assert hasattr(manager, 'convert_kzt_to_eur'), "CurrencyManager should have convert_kzt_to_eur method"
        assert hasattr(manager, 'convert_kzt_to_usd'), "CurrencyManager should have convert_kzt_to_usd method"
