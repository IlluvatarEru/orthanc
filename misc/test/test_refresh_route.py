
"""
Test script to verify the refresh_analysis route works correctly.
"""

from webapp import app
import json

def test_refresh_route():
    """Test the refresh_analysis route."""
    print("🧪 Testing refresh_analysis route")
    print("=" * 50)
    
    with app.test_client() as client:
        # Test the refresh route for Jazz-квартал
        complex_name = "Jazz-квартал"
        response = client.post(f'/refresh_analysis/{complex_name}')
        
        print(f"📊 Response status: {response.status_code}")
        print(f"📊 Response headers: {dict(response.headers)}")
        
        try:
            data = response.get_json()
            print(f"📊 Response data: {json.dumps(data, indent=2)}")
            
            if data.get('success'):
                print("✅ Refresh route is working correctly!")
                print(f"   Rental count: {data.get('rental_count', 0)}")
                print(f"   Sales count: {data.get('sales_count', 0)}")
            else:
                print("❌ Refresh route failed:")
                print(f"   Error: {data.get('error', 'Unknown error')}")
                
        except Exception as e:
            print(f"❌ Error parsing response: {e}")
            print(f"📄 Raw response: {response.data}")


if __name__ == "__main__":
    test_refresh_route() 