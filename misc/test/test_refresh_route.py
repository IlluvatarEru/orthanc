
"""
Test script to verify the refresh_analysis route works correctly.
"""

from webapp import app
import json
import logging
def test_refresh_route():
    """Test the refresh_analysis route."""
    logging.info("Testing refresh_analysis route")
    logging.info("=" * 50)
    
    with app.test_client() as client:
        # Test the refresh route for Jazz-квартал
        complex_name = "Jazz-квартал"
        response = client.post(f'/refresh_analysis/{complex_name}')
        
        logging.info(f"Response status: {response.status_code}")
        logging.info(f"Response headers: {dict(response.headers)}")
        
        try:
            data = response.get_json()
            logging.info(f"Response data: {json.dumps(data, indent=2)}")
            
            if data.get('success'):
                logging.info("Refresh route is working correctly!")
                logging.info(f"   Rental count: {data.get('rental_count', 0)}")
                logging.info(f"   Sales count: {data.get('sales_count', 0)}")
            else:
                logging.info("Refresh route failed:")
                logging.info(f"   Error: {data.get('error', 'Unknown error')}")
                
        except Exception as e:
            logging.info(f"Error parsing response: {e}")
            logging.info(f"Raw response: {response.data}")


if __name__ == "__main__":
    test_refresh_route() 