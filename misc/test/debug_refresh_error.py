
"""
Debug script to test refresh route and identify the exact error.
"""
from analytics.src.old_jk_analytics import JKAnalytics
from webapp import app
import json
import requests
import logging
def test_refresh_with_requests():
    """Test the refresh route using requests to see the actual response."""
    logging.info("Testing refresh route with requests")
    logging.info("=" * 50)
    
    # Test with a complex that should work
    complex_name = "Jazz-квартал"
    
    # Make the request
    url = f"http://localhost:5000/refresh_analysis/{complex_name}"
    logging.info(f"📡 Making request to: {url}")
    
    try:
        response = requests.post(url, timeout=300)  # 5 minute timeout
        logging.info(f"Response status: {response.status_code}")
        logging.info(f"Response headers: {dict(response.headers)}")
        logging.info(f"Response text: {response.text[:1000]}...")  # First 1000 chars
        
        if response.status_code == 200:
            try:
                data = response.json()
                logging.info(f"JSON response: {json.dumps(data, indent=2)}")
                
                if data.get('success'):
                    logging.info("Refresh successful!")
                else:
                    logging.info(f"Refresh failed: {data.get('error')}")
            except json.JSONDecodeError as e:
                logging.info(f"Invalid JSON response: {e}")
        else:
            logging.info(f"HTTP error: {response.status_code}")
            
    except requests.exceptions.RequestException as e:
        logging.info(f"Request failed: {e}")

def test_refresh_with_test_client():
    """Test the refresh route using Flask test client."""
    logging.info(f"\nTesting refresh route with Flask test client")
    logging.info("=" * 50)
    
    with app.test_client() as client:
        complex_name = "Jazz-квартал"
        response = client.post(f'/refresh_analysis/{complex_name}')
        
        logging.info(f"Response status: {response.status_code}")
        logging.info(f"Response headers: {dict(response.headers)}")
        
        try:
            data = response.get_json()
            logging.info(f"Response data: {json.dumps(data, indent=2)}")
            
            if data.get('success'):
                logging.info("Refresh successful!")
            else:
                logging.info(f"Refresh failed: {data.get('error')}")
                
        except Exception as e:
            logging.info(f"Error parsing response: {e}")
            logging.info(f"Raw response: {response.data}")

def test_analytics_function():
    """Test the analytics function directly."""
    logging.info(f"\nTesting analytics function directly")
    logging.info("=" * 50)
    
    try:
        analytics = JKAnalytics()
        
        # Test with Jazz-квартал
        result = analytics.get_jk_comprehensive_analysis('Jazz-квартал', 1000.0, '2024-01-01')
        
        logging.info(f"Analytics result: {json.dumps(result, indent=2)}")
        
        if 'error' in result:
            logging.info(f"Analytics error: {result['error']}")
        else:
            logging.info("Analytics successful!")
            
    except Exception as e:
        logging.info(f"Analytics error: {e}")

if __name__ == "__main__":
    test_refresh_with_test_client()
    test_analytics_function() 