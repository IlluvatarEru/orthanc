"""
Test script for currency functionality.
"""
from common.src.currency import currency_manager, format_price_with_eur
import logging

def test_currency_functionality():
    """Test the currency conversion functionality."""
    
    logging.info("Testing Currency Functionality")
    logging.info("=" * 40)
    
    # Test 1: Update exchange rates
    logging.info("1. Updating exchange rates from mig.kz...")
    success = currency_manager.update_exchange_rates()
    logging.info(f"   Result: {'Success' if success else 'Failed'}")
    
    # Test 2: Get latest rates
    logging.info("\n2. Getting latest exchange rates...")
    eur_rate = currency_manager.get_latest_rate('EUR')
    usd_rate = currency_manager.get_latest_rate('USD')
    logging.info(f"   EUR rate: {eur_rate}")
    logging.info(f"   USD rate: {usd_rate}")
    
    # Test 3: Convert some sample prices
    logging.info("\n3. Testing price conversions...")
    sample_prices = [1000000, 5000000, 15000000, 50000000]
    
    for price in sample_prices:
        eur_amount = currency_manager.convert_kzt_to_eur(price)
        usd_amount = currency_manager.convert_kzt_to_usd(price)
        
        logging.info(f"   ₸{price:,} = €{eur_amount:,.0f} = ${usd_amount:,.0f}")
    
    # Test 4: Test formatting function
    logging.info("\n4. Testing price formatting...")
    for price in sample_prices:
        formatted = format_price_with_eur(price, currency_manager, show_eur=True)
        logging.info(f"   {formatted}")
    
    logging.info("\nCurrency functionality test completed!")

if __name__ == "__main__":
    test_currency_functionality() 