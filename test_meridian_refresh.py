#!/usr/bin/env python3
"""
Test script to verify the refresh_analysis route works for Meridian Apartments.
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from webapp import app
import json

def test_meridian_refresh():
    """Test the refresh_analysis route for Meridian Apartments."""
    print("🧪 Testing refresh_analysis route for Meridian Apartments")
    print("=" * 60)
    
    with app.test_client() as client:
        # Test the refresh route for Meridian Apartments
        complex_name = "Meridian Apartments"
        response = client.post(f'/refresh_analysis/{complex_name}')
        
        print(f"📊 Response status: {response.status_code}")
        print(f"📊 Response headers: {dict(response.headers)}")
        
        try:
            data = response.get_json()
            print(f"📊 Response data: {json.dumps(data, indent=2)}")
            
            if data.get('success'):
                print("✅ Refresh route is working correctly for Meridian Apartments!")
                print(f"   Rental count: {data.get('rental_count', 0)}")
                print(f"   Sales count: {data.get('sales_count', 0)}")
            else:
                print("❌ Refresh route failed for Meridian Apartments:")
                print(f"   Error: {data.get('error', 'Unknown error')}")
                
        except Exception as e:
            print(f"❌ Error parsing response: {e}")
            print(f"📄 Raw response: {response.data}")


if __name__ == "__main__":
    test_meridian_refresh() 