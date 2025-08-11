
"""
Debug script to test refresh route and identify the exact error.
"""
from analytics.src.jk_analytics import JKAnalytics
from webapp import app
import json
import requests

def test_refresh_with_requests():
    """Test the refresh route using requests to see the actual response."""
    print("🔍 Testing refresh route with requests")
    print("=" * 50)
    
    # Test with a complex that should work
    complex_name = "Jazz-квартал"
    
    # Make the request
    url = f"http://localhost:5000/refresh_analysis/{complex_name}"
    print(f"📡 Making request to: {url}")
    
    try:
        response = requests.post(url, timeout=300)  # 5 minute timeout
        print(f"📊 Response status: {response.status_code}")
        print(f"📊 Response headers: {dict(response.headers)}")
        print(f"📄 Response text: {response.text[:1000]}...")  # First 1000 chars
        
        if response.status_code == 200:
            try:
                data = response.json()
                print(f"📊 JSON response: {json.dumps(data, indent=2)}")
                
                if data.get('success'):
                    print("✅ Refresh successful!")
                else:
                    print(f"❌ Refresh failed: {data.get('error')}")
            except json.JSONDecodeError as e:
                print(f"❌ Invalid JSON response: {e}")
        else:
            print(f"❌ HTTP error: {response.status_code}")
            
    except requests.exceptions.RequestException as e:
        print(f"❌ Request failed: {e}")

def test_refresh_with_test_client():
    """Test the refresh route using Flask test client."""
    print(f"\n🧪 Testing refresh route with Flask test client")
    print("=" * 50)
    
    with app.test_client() as client:
        complex_name = "Jazz-квартал"
        response = client.post(f'/refresh_analysis/{complex_name}')
        
        print(f"📊 Response status: {response.status_code}")
        print(f"📊 Response headers: {dict(response.headers)}")
        
        try:
            data = response.get_json()
            print(f"📊 Response data: {json.dumps(data, indent=2)}")
            
            if data.get('success'):
                print("✅ Refresh successful!")
            else:
                print(f"❌ Refresh failed: {data.get('error')}")
                
        except Exception as e:
            print(f"❌ Error parsing response: {e}")
            print(f"📄 Raw response: {response.data}")

def test_analytics_function():
    """Test the analytics function directly."""
    print(f"\n📊 Testing analytics function directly")
    print("=" * 50)
    
    try:
        analytics = JKAnalytics()
        
        # Test with Jazz-квартал
        result = analytics.get_jk_comprehensive_analysis('Jazz-квартал', 1000.0, '2024-01-01')
        
        print(f"📊 Analytics result: {json.dumps(result, indent=2)}")
        
        if 'error' in result:
            print(f"❌ Analytics error: {result['error']}")
        else:
            print("✅ Analytics successful!")
            
    except Exception as e:
        print(f"❌ Analytics error: {e}")

if __name__ == "__main__":
    test_refresh_with_test_client()
    test_analytics_function() 