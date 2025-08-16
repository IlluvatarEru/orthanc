
"""
Test script to verify the refresh_analysis route works for Meridian Apartments.
"""

from webapp import app
import json
import logging
def test_meridian_refresh():
    """Test the refresh_analysis route for Meridian Apartments."""
    logging.info("Testing refresh_analysis route for Meridian Apartments")
    logging.info("=" * 60)
    
    with app.test_client() as client:
        # Test the refresh route for Meridian Apartments
        complex_name = "Meridian Apartments"
        response = client.post(f'/refresh_analysis/{complex_name}')
        
        logging.info(f"Response status: {response.status_code}")
        logging.info(f"Response headers: {dict(response.headers)}")
        
        try:
            data = response.get_json()
            logging.info(f"Response data: {json.dumps(data, indent=2)}")
            
            if data.get('success'):
                logging.info("Refresh route is working correctly for Meridian Apartments!")
                logging.info(f"   Rental count: {data.get('rental_count', 0)}")
                logging.info(f"   Sales count: {data.get('sales_count', 0)}")
            else:
                logging.info("Refresh route failed for Meridian Apartments:")
                logging.info(f"   Error: {data.get('error', 'Unknown error')}")
                
        except Exception as e:
            logging.info(f"Error parsing response: {e}")
            logging.info(f"Raw response: {response.data}")


if __name__ == "__main__":
    test_meridian_refresh() 