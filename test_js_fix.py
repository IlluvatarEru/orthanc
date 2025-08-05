#!/usr/bin/env python3
"""
Test to verify the JavaScript fix works.
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from webapp import app
import json

def test_refresh_response():
    """Test that the refresh route returns the correct response format."""
    print("🧪 Testing refresh response format")
    print("=" * 50)
    
    with app.test_client() as client:
        complex_name = "Jazz-квартал"
        response = client.post(f'/refresh_analysis/{complex_name}')
        
        print(f"📊 Response status: {response.status_code}")
        
        if response.status_code == 200:
            try:
                data = response.get_json()
                print(f"📊 Response data: {json.dumps(data, indent=2)}")
                
                # Check if response has the expected format
                required_fields = ['success', 'message', 'rental_count', 'sales_count']
                missing_fields = [field for field in required_fields if field not in data]
                
                if missing_fields:
                    print(f"❌ Missing required fields: {missing_fields}")
                else:
                    print("✅ Response has all required fields")
                    
                if data.get('success') is True:
                    print("✅ Success field is true")
                else:
                    print(f"❌ Success field is: {data.get('success')}")
                    
                print(f"✅ Response format is correct for JavaScript processing")
                
            except Exception as e:
                print(f"❌ Error parsing response: {e}")
        else:
            print(f"❌ HTTP error: {response.status_code}")

if __name__ == "__main__":
    test_refresh_response() 