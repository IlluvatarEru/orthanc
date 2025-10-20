"""
Currency conversion module for fetching exchange rates.

This module provides functionality to fetch EUR and USD to KZT exchange rates
from mig.kz and perform currency conversions.
"""
import re
from typing import Optional
import logging
import requests


class CurrencyManager:
    """
    Manages currency conversion and exchange rate fetching.
    """

    def __init__(self):
        """
        Initialize currency manager.
        """
        pass

    def fetch_mig_exchange_rates(self) -> dict[str, float]:
        """
        Fetch EUR and USD to KZT exchange rates from mig.kz.
        
        :return: dict[str, float], dictionary with currency codes as keys and rates as values
        """
        try:
            # Fetch the mig.kz page
            response = requests.get('https://mig.kz/', timeout=10)
            response.raise_for_status()

            content = response.text
            rates = {}

            # Parse the HTML table structure for exchange rates
            # Look for table rows with currency data
            table_pattern = r'<tr>.*?<td[^>]*>([\d.]+)</td>.*?<td[^>]*class="currency"[^>]*>([A-Z]+)</td>.*?<td[^>]*>([\d.]+)</td>.*?</tr>'
            table_matches = re.findall(table_pattern, content, re.DOTALL | re.IGNORECASE)

            for match in table_matches:
                buy_rate = float(match[0])
                currency = match[1].upper()
                sell_rate = float(match[2])
                
                # Use the average of buy and sell rates for mid-price
                mid_rate = (buy_rate + sell_rate) / 2
                
                # Validate rate is within reasonable range
                if 100 < mid_rate < 1000:
                    rates[currency] = mid_rate
                    logging.info(f"Found {currency} rate: {mid_rate} (buy: {buy_rate}, sell: {sell_rate})")

            # If we didn't find rates in table format, try alternative patterns
            if not rates:
                logging.info("Could not find rates in table format, trying alternative patterns.")
                
                # Look for patterns like "USD" followed by numbers
                currency_patterns = [
                    r'USD[^>]*>.*?(\d+\.?\d*)',
                    r'EUR[^>]*>.*?(\d+\.?\d*)',
                ]
                
                for pattern in currency_patterns:
                    matches = re.findall(pattern, content, re.IGNORECASE)
                    for match in matches:
                        rate = float(match)
                        if 100 < rate < 1000:
                            # Determine currency from pattern
                            if 'USD' in pattern.upper():
                                rates['USD'] = rate
                            elif 'EUR' in pattern.upper():
                                rates['EUR'] = rate

            # Final fallback: look for any reasonable numbers
            if not rates:
                logging.info("Could not find rates with specific patterns, trying generic number extraction.")
                # Look for any number that could be a rate
                number_pattern = r'(\d+\.?\d*)'
                numbers = re.findall(number_pattern, content)
                
                # Filter for reasonable exchange rates
                reasonable_rates = []
                for num in numbers:
                    rate = float(num)
                    if 100 < rate < 1000:
                        reasonable_rates.append(rate)
                
                # Take the first few reasonable rates and assign to currencies
                if len(reasonable_rates) >= 2:
                    rates['USD'] = reasonable_rates[0]
                    rates['EUR'] = reasonable_rates[1]

            return rates

        except Exception as e:
            logging.error(f"Error fetching exchange rates from mig.kz: {e}")
            return {}

    def get_latest_rate(self, currency: str) -> Optional[float]:
        """
        Get the latest exchange rate for a currency by fetching from web.
        
        :param currency: str, currency code (EUR or USD)
        :return: Optional[float], latest rate or None if not found
        """
        rates = self.fetch_mig_exchange_rates()
        return rates.get(currency)

    def convert_kzt_to_eur(self, kzt_amount: float) -> Optional[float]:
        """
        Convert KZT amount to EUR.
        
        :param kzt_amount: float, amount in KZT
        :return: Optional[float], amount in EUR or None if conversion failed
        """
        eur_rate = self.get_latest_rate('EUR')
        if eur_rate:
            return kzt_amount / eur_rate
        return None

    def convert_kzt_to_usd(self, kzt_amount: float) -> Optional[float]:
        """
        Convert KZT amount to USD.
        
        :param kzt_amount: float, amount in KZT
        :return: Optional[float], amount in USD or None if conversion failed
        """
        usd_rate = self.get_latest_rate('USD')
        if usd_rate:
            return kzt_amount / usd_rate
        return None

    def convert_kzt_to_gbp(self, kzt_amount: float) -> Optional[float]:
        """
        Convert KZT amount to GBP.
        
        :param kzt_amount: float, amount in KZT
        :return: Optional[float], amount in GBP or None if conversion failed
        """
        gbp_rate = self.get_latest_rate('GBP')
        if gbp_rate:
            return kzt_amount / gbp_rate
        return None


def format_price_with_eur(kzt_amount: float, currency_manager: CurrencyManager, show_eur: bool = False) -> str:
    """
    Format KZT price with optional EUR conversion.
    
    :param kzt_amount: float, amount in KZT
    :param currency_manager: CurrencyManager, currency manager instance
    :param show_eur: bool, whether to show EUR conversion
    :return: str, formatted price string
    """
    kzt_formatted = f"₸{kzt_amount:,.0f}"

    if show_eur:
        eur_amount = currency_manager.convert_kzt_to_eur(kzt_amount)
        if eur_amount:
            return f"{kzt_formatted} (€{eur_amount:.0f})"

    return kzt_formatted


def format_price_with_currency(kzt_amount: float, currency_manager: CurrencyManager, currency: str = 'EUR') -> str:
    """
    Format KZT price with currency conversion.
    
    :param kzt_amount: float, amount in KZT
    :param currency_manager: CurrencyManager, currency manager instance
    :param currency: str, currency code (EUR, USD, GBP)
    :return: str, formatted price string
    """
    kzt_formatted = f"₸{kzt_amount:,.0f}"
    
    if currency.upper() == 'EUR':
        converted = currency_manager.convert_kzt_to_eur(kzt_amount)
        symbol = '€'
    elif currency.upper() == 'USD':
        converted = currency_manager.convert_kzt_to_usd(kzt_amount)
        symbol = '$'
    elif currency.upper() == 'GBP':
        converted = currency_manager.convert_kzt_to_gbp(kzt_amount)
        symbol = '£'
    else:
        return kzt_formatted
    
    if converted:
        return f"{kzt_formatted} ({symbol}{converted:.0f})"
    
    return kzt_formatted


# Global currency manager instance
currency_manager = CurrencyManager()
