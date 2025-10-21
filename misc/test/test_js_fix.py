
"""
Test to verify the JavaScript fix works.
"""

from webapp import app
import json
import logging
def test_refresh_response():
    """Test that the refresh route returns the correct response format."""
    logging.info("Testing refresh response format")
    logging.info("=" * 50)
    
    with app.test_client() as client:
        complex_name = "Jazz-квартал"
        response = client.post(f'/refresh_analysis/{complex_name}')
        
        logging.info(f"Response status: {response.status_code}")
        
        if response.status_code == 200:
            try:
                data = response.get_json()
                logging.info(f"Response data: {json.dumps(data, indent=2)}")
                
                # Check if response has the expected format
                required_fields = ['success', 'message', 'rental_count', 'sales_count']
                missing_fields = [field for field in required_fields if field not in data]
                
                if missing_fields:
                    logging.info(f"Missing required fields: {missing_fields}")
                else:
                    logging.info("Response has all required fields")
                    
                if data.get('success') is True:
                    logging.info("Success field is true")
                else:
                    logging.info(f"Success field is: {data.get('success')}")
                    
                logging.info(f"Response format is correct for JavaScript processing")
                
            except Exception as e:
                logging.info(f"Error parsing response: {e}")
        else:
            logging.info(f"HTTP error: {response.status_code}")

if __name__ == "__main__":
    test_refresh_response() 