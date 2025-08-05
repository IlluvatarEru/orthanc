"""
Currency conversion module for fetching and managing exchange rates.

This module provides functionality to fetch EUR and USD to KZT exchange rates
from mig.kz and store them in the database.
"""

import sqlite3
import requests
from datetime import datetime
from typing import Optional, Dict
import re


class CurrencyManager:
    """
    Manages currency conversion and exchange rate fetching.
    """
    
    def __init__(self, db_path: str = "flats.db"):
        """
        Initialize currency manager.
        
        :param db_path: str, path to SQLite database file
        """
        self.db_path = db_path
        self.conn = None
        self.create_mid_prices_table()
    
    def connect(self):
        """
        Establish database connection.
        """
        if self.conn is None:
            self.conn = sqlite3.connect(self.db_path)
            self.conn.row_factory = sqlite3.Row
    
    def disconnect(self):
        """
        Close database connection.
        """
        if self.conn:
            self.conn.close()
            self.conn = None
    
    def create_mid_prices_table(self):
        """
        Create mid_prices table if it doesn't exist.
        """
        self.connect()
        
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS mid_prices (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                currency TEXT NOT NULL,
                rate REAL NOT NULL,
                fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Create index for faster queries
        self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_currency_fetched ON mid_prices(currency, fetched_at)
        """)
        
        self.conn.commit()
        self.disconnect()
    
    def fetch_mig_exchange_rates(self) -> Dict[str, float]:
        """
        Fetch EUR and USD to KZT exchange rates from mig.kz.
        
        :return: Dict[str, float], dictionary with currency codes as keys and rates as values
        """
        try:
            # Fetch the mig.kz page
            response = requests.get('https://mig.kz/', timeout=10)
            response.raise_for_status()
            
            content = response.text
            
            # Look for actual exchange rate data in the page
            # The rates are typically displayed in a table or specific format
            rates = {}
            
            # Look for EUR rate - search for patterns like "543.24 тенге" near EUR
            eur_patterns = [
                r'EUR[^>]*>.*?(\d+\.?\d*)\s*тенге',
                r'(\d+\.?\d*)\s*тенге.*?EUR',
                r'евро[^>]*>.*?(\d+\.?\d*)',
                r'(\d+\.?\d*).*?евро'
            ]
            
            for pattern in eur_patterns:
                eur_match = re.search(pattern, content, re.IGNORECASE)
                if eur_match:
                    try:
                        eur_rate = float(eur_match.group(1))
                        if 400 < eur_rate < 700:  # Reasonable range for EUR/KZT
                            rates['EUR'] = eur_rate
                            break
                    except ValueError:
                        continue
            
            # Look for USD rate - search for patterns like "619.89 тенге" near USD
            usd_patterns = [
                r'USD[^>]*>.*?(\d+\.?\d*)\s*тенге',
                r'(\d+\.?\d*)\s*тенге.*?USD',
                r'доллар[^>]*>.*?(\d+\.?\d*)',
                r'(\d+\.?\d*).*?доллар'
            ]
            
            for pattern in usd_patterns:
                usd_match = re.search(pattern, content, re.IGNORECASE)
                if usd_match:
                    try:
                        usd_rate = float(usd_match.group(1))
                        if 400 < usd_rate < 700:  # Reasonable range for USD/KZT
                            rates['USD'] = usd_rate
                            break
                    except ValueError:
                        continue
            
            # If we still don't have rates, try to find them in the visible text
            if not rates:
                # Look for any number followed by "тенге" that could be a rate
                tenge_pattern = r'(\d+\.?\d*)\s*тенге'
                tenge_matches = re.findall(tenge_pattern, content)
                
                # Filter for reasonable exchange rates (400-700 range)
                reasonable_rates = [float(rate) for rate in tenge_matches if 400 < float(rate) < 700]
                
                if len(reasonable_rates) >= 2:
                    # Assume first two reasonable rates are USD and EUR
                    rates['USD'] = reasonable_rates[0]
                    rates['EUR'] = reasonable_rates[1]
            
            return rates
            
        except Exception as e:
            print(f"Error fetching exchange rates from mig.kz: {e}")
            return {}
    
    def update_exchange_rates(self) -> bool:
        """
        Fetch and store latest exchange rates.
        
        :return: bool, True if successful, False otherwise
        """
        try:
            rates = self.fetch_mig_exchange_rates()
            
            if not rates:
                print("No exchange rates fetched")
                return False
            
            self.connect()
            
            for currency, rate in rates.items():
                self.conn.execute("""
                    INSERT INTO mid_prices (currency, rate, fetched_at)
                    VALUES (?, ?, ?)
                """, (currency, rate, datetime.now()))
            
            self.conn.commit()
            self.disconnect()
            
            print(f"Updated exchange rates: {rates}")
            return True
            
        except Exception as e:
            print(f"Error updating exchange rates: {e}")
            return False
    
    def get_latest_rate(self, currency: str) -> Optional[float]:
        """
        Get the latest exchange rate for a currency.
        
        :param currency: str, currency code (EUR or USD)
        :return: Optional[float], latest rate or None if not found
        """
        try:
            self.connect()
            
            cursor = self.conn.execute("""
                SELECT rate FROM mid_prices 
                WHERE currency = ? 
                ORDER BY fetched_at DESC 
                LIMIT 1
            """, (currency,))
            
            result = cursor.fetchone()
            self.disconnect()
            
            return float(result['rate']) if result else None
            
        except Exception as e:
            print(f"Error getting latest rate for {currency}: {e}")
            return None
    
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


# Global currency manager instance
currency_manager = CurrencyManager() 