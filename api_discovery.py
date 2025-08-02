"""
API Discovery Tool for Krisha.kz

This tool helps discover the API endpoints used by Krisha.kz to load flat listings.
"""

import requests
import re
import json
from urllib.parse import urlparse, parse_qs
from typing import List, Dict, Optional


def analyze_search_page(url: str) -> Dict:
    """
    Analyze a Krisha.kz search page to find API endpoints.
    
    :param url: str, search page URL
    :return: Dict, discovered API endpoints and parameters
    """
    print(f"🔍 Analyzing search page: {url}")
    
    # Headers to mimic browser
    headers = {
        'User-Agent': 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Mobile Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
    }
    
    try:
        # Fetch the search page
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        
        html_content = response.text
        
        # Look for API endpoints in the HTML
        api_endpoints = find_api_endpoints(html_content)
        
        # Look for JavaScript variables that might contain API data
        js_data = extract_js_data(html_content)
        
        # Parse URL parameters
        url_params = parse_search_parameters(url)
        
        return {
            'url': url,
            'api_endpoints': api_endpoints,
            'js_data': js_data,
            'url_params': url_params,
            'html_length': len(html_content)
        }
        
    except Exception as e:
        print(f"❌ Error analyzing page: {e}")
        return {'error': str(e)}


def find_api_endpoints(html_content: str) -> List[str]:
    """
    Find API endpoints in HTML content.
    
    :param html_content: str, HTML content to analyze
    :return: List[str], found API endpoints
    """
    endpoints = []
    
    # Common patterns for API endpoints
    patterns = [
        r'https?://[^"\s]+/api/[^"\s]+',
        r'https?://[^"\s]+/analytics/[^"\s]+',
        r'https?://[^"\s]+/search/[^"\s]+',
        r'https?://[^"\s]+/listings/[^"\s]+',
        r'https?://[^"\s]+/flats/[^"\s]+',
        r'https?://[^"\s]+/arenda/[^"\s]+',
        r'https?://[^"\s]+/prodazha/[^"\s]+',
    ]
    
    for pattern in patterns:
        matches = re.findall(pattern, html_content)
        endpoints.extend(matches)
    
    # Remove duplicates and filter krisha.kz domains
    unique_endpoints = []
    for endpoint in set(endpoints):
        if 'krisha.kz' in endpoint:
            unique_endpoints.append(endpoint)
    
    return unique_endpoints


def extract_js_data(html_content: str) -> Dict:
    """
    Extract JavaScript data that might contain API information.
    
    :param html_content: str, HTML content to analyze
    :return: Dict, extracted JavaScript data
    """
    js_data = {}
    
    # Look for JSON data in script tags
    json_patterns = [
        r'<script[^>]*>window\.__INITIAL_STATE__\s*=\s*({[^<]+})</script>',
        r'<script[^>]*>window\.__DATA__\s*=\s*({[^<]+})</script>',
        r'<script[^>]*>var\s+data\s*=\s*({[^<]+})</script>',
        r'<script[^>]*>const\s+apiData\s*=\s*({[^<]+})</script>',
    ]
    
    for pattern in json_patterns:
        matches = re.findall(pattern, html_content, re.DOTALL)
        for match in matches:
            try:
                data = json.loads(match)
                js_data['initial_state'] = data
            except json.JSONDecodeError:
                pass
    
    # Look for API URLs in JavaScript
    api_url_patterns = [
        r'apiUrl["\']?\s*[:=]\s*["\']([^"\']+)["\']',
        r'endpoint["\']?\s*[:=]\s*["\']([^"\']+)["\']',
        r'url["\']?\s*[:=]\s*["\']([^"\']+)["\']',
    ]
    
    for pattern in api_url_patterns:
        matches = re.findall(pattern, html_content)
        if matches:
            js_data['api_urls'] = matches
    
    return js_data


def parse_search_parameters(url: str) -> Dict:
    """
    Parse search parameters from URL.
    
    :param url: str, search URL
    :return: Dict, parsed parameters
    """
    parsed = urlparse(url)
    params = parse_qs(parsed.query)
    
    # Convert single values to strings
    result = {}
    for key, value in params.items():
        if len(value) == 1:
            result[key] = value[0]
        else:
            result[key] = value
    
    return result


def test_api_endpoints(endpoints: List[str], search_params: Dict) -> List[Dict]:
    """
    Test discovered API endpoints with search parameters.
    
    :param endpoints: List[str], API endpoints to test
    :param search_params: Dict, search parameters
    :return: List[Dict], test results
    """
    results = []
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Mobile Safari/537.36',
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
        'X-Requested-With': 'XMLHttpRequest',
    }
    
    for endpoint in endpoints:
        try:
            print(f"🧪 Testing endpoint: {endpoint}")
            
            # Try different HTTP methods
            for method in ['GET', 'POST']:
                try:
                    if method == 'GET':
                        response = requests.get(endpoint, headers=headers, timeout=10)
                    else:
                        response = requests.post(endpoint, headers=headers, timeout=10)
                    
                    if response.status_code == 200:
                        content_type = response.headers.get('content-type', '')
                        
                        result = {
                            'endpoint': endpoint,
                            'method': method,
                            'status_code': response.status_code,
                            'content_type': content_type,
                            'content_length': len(response.content),
                            'is_json': 'json' in content_type.lower(),
                        }
                        
                        if result['is_json']:
                            try:
                                json_data = response.json()
                                result['json_keys'] = list(json_data.keys()) if isinstance(json_data, dict) else []
                                result['sample_data'] = str(json_data)[:200] + '...' if len(str(json_data)) > 200 else str(json_data)
                            except json.JSONDecodeError:
                                result['json_error'] = True
                        
                        results.append(result)
                        print(f"✅ {method} {endpoint} - Status: {response.status_code}")
                        
                except requests.exceptions.RequestException as e:
                    print(f"❌ {method} {endpoint} - Error: {e}")
                    
        except Exception as e:
            print(f"❌ Error testing {endpoint}: {e}")
    
    return results


def generate_api_calls(search_params: Dict) -> List[str]:
    """
    Generate potential API calls based on search parameters.
    
    :param search_params: Dict, search parameters
    :return: List[str], potential API calls
    """
    api_calls = []
    
    # Common API patterns for Krisha.kz
    base_patterns = [
        'https://krisha.kz/api/search',
        'https://krisha.kz/api/listings',
        'https://krisha.kz/api/flats',
        'https://krisha.kz/api/arenda',
        'https://m.krisha.kz/api/search',
        'https://m.krisha.kz/api/listings',
        'https://m.krisha.kz/api/flats',
        'https://m.krisha.kz/api/arenda',
    ]
    
    for base_url in base_patterns:
        # Convert search parameters to API format
        api_params = {}
        
        # Map URL parameters to API parameters
        if 'das[live.rooms]' in search_params:
            api_params['rooms'] = search_params['das[live.rooms]']
        
        if 'das[live.square][to]' in search_params:
            api_params['area_max'] = search_params['das[live.square][to]']
        
        if 'das[map.complex]' in search_params:
            api_params['complex_id'] = search_params['das[map.complex]']
        
        # Build API call
        if api_params:
            param_str = '&'.join([f"{k}={v}" for k, v in api_params.items()])
            api_calls.append(f"{base_url}?{param_str}")
        else:
            api_calls.append(base_url)
    
    return api_calls


def main():
    """
    Main function to discover API endpoints.
    """
    # Test URL from your query
    test_url = "https://krisha.kz/arenda/kvartiry/almaty/?das[live.rooms]=1&das[live.square][to]=35&das[map.complex]=2758"
    
    print("🔍 Krisha.kz API Discovery Tool")
    print("=" * 50)
    
    # Analyze the search page
    analysis = analyze_search_page(test_url)
    
    if 'error' in analysis:
        print(f"❌ Analysis failed: {analysis['error']}")
        return
    
    print(f"\n📊 Analysis Results:")
    print(f"HTML Length: {analysis['html_length']:,} characters")
    print(f"URL Parameters: {analysis['url_params']}")
    
    if analysis['api_endpoints']:
        print(f"\n🔗 Found {len(analysis['api_endpoints'])} API endpoints:")
        for endpoint in analysis['api_endpoints']:
            print(f"   - {endpoint}")
    
    if analysis['js_data']:
        print(f"\n📜 JavaScript Data Found:")
        for key, value in analysis['js_data'].items():
            print(f"   - {key}: {type(value).__name__}")
    
    # Test discovered endpoints
    if analysis['api_endpoints']:
        print(f"\n🧪 Testing {len(analysis['api_endpoints'])} endpoints...")
        test_results = test_api_endpoints(analysis['api_endpoints'], analysis['url_params'])
        
        print(f"\n✅ Successful API calls:")
        for result in test_results:
            if result['status_code'] == 200:
                print(f"   {result['method']} {result['endpoint']}")
                if result['is_json'] and 'json_keys' in result:
                    print(f"      JSON keys: {result['json_keys']}")
    
    # Generate potential API calls
    print(f"\n🔧 Generated API calls:")
    api_calls = generate_api_calls(analysis['url_params'])
    for call in api_calls:
        print(f"   - {call}")
    
    print(f"\n💡 Next Steps:")
    print("1. Test the generated API calls manually")
    print("2. Check browser Network tab for actual API calls")
    print("3. Look for XHR/Fetch requests in browser dev tools")
    print("4. Monitor network traffic while loading the search page")


if __name__ == "__main__":
    main() 