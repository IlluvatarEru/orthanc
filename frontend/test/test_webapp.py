#!/usr/bin/env python3
"""
Test script for the webapp frontend.
"""
import requests
import time

def test_webapp_connectivity():
    """Test if webapp is running and responding."""
    try:
        response = requests.get("http://localhost:5000/", timeout=5)
        if response.status_code == 200:
            print("✅ Webapp is running and responding")
            return True
        else:
            print(f"❌ Webapp returned status {response.status_code}")
            return False
    except requests.exceptions.ConnectionError:
        print("❌ Webapp is not running on localhost:5000")
        return False
    except Exception as e:
        print(f"❌ Error testing webapp: {e}")
        return False

def test_api_connectivity():
    """Test if API server is running."""
    try:
        response = requests.get("http://localhost:8000/api/health", timeout=5)
        if response.status_code == 200:
            print("✅ API server is running and responding")
            return True
        else:
            print(f"❌ API server returned status {response.status_code}")
            return False
    except requests.exceptions.ConnectionError:
        print("❌ API server is not running on localhost:8000")
        return False
    except Exception as e:
        print(f"❌ Error testing API server: {e}")
        return False

def test_analyze_jk_endpoint():
    """Test the analyze_jk endpoint."""
    try:
        response = requests.get("http://localhost:5000/analyze_jk/Meridian%20Apartments", timeout=10)
        if response.status_code == 200:
            print("✅ Analyze JK endpoint is working")
            return True
        else:
            print(f"❌ Analyze JK endpoint returned status {response.status_code}")
            return False
    except Exception as e:
        print(f"❌ Error testing analyze_jk endpoint: {e}")
        return False

def main():
    """Run all tests."""
    print("Testing Orthanc webapp frontend...")
    print()
    
    # Test webapp connectivity
    print("1. Testing webapp connectivity...")
    webapp_ok = test_webapp_connectivity()
    print()
    
    # Test API connectivity
    print("2. Testing API server connectivity...")
    api_ok = test_api_connectivity()
    print()
    
    # Test analyze_jk endpoint
    if webapp_ok:
        print("3. Testing analyze_jk endpoint...")
        endpoint_ok = test_analyze_jk_endpoint()
        print()
    else:
        endpoint_ok = False
    
    # Summary
    print("=== SUMMARY ===")
    print(f"Webapp: {'✅ OK' if webapp_ok else '❌ FAILED'}")
    print(f"API Server: {'✅ OK' if api_ok else '❌ FAILED'}")
    print(f"Analyze JK: {'✅ OK' if endpoint_ok else '❌ FAILED'}")
    
    if not webapp_ok:
        print()
        print("To start the webapp:")
        print("  python -m frontend.launch.launch_webapp")
    
    if not api_ok:
        print()
        print("To start the API server:")
        print("  python -m api.launch.launch_api")

if __name__ == '__main__':
    main()
