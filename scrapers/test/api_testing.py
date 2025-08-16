"""
API Testing Tool for Krisha.kz

This tool tests various API endpoints to find the correct one for listing flats.
"""

import requests
import json
import time
from typing import List, Dict, Optional

import logging
def test_search_api_endpoints(search_params: Dict) -> List[Dict]:
    """
    Test various search API endpoints with the given parameters.
    
    :param search_params: Dict, search parameters
    :return: List[Dict], test results
    """
    results = []
    
    # Headers to mimic browser
    headers = {
        'User-Agent': 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Mobile Safari/537.36',
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
        'X-Requested-With': 'XMLHttpRequest',
        'Referer': 'https://krisha.kz/arenda/kvartiry/almaty/',
        'Origin': 'https://krisha.kz',
    }
    
    # Test endpoints based on common patterns
    test_endpoints = [
        # Mobile API endpoints (similar to what we used for individual flats)
        {
            'url': 'https://m.krisha.kz/api/search',
            'method': 'POST',
            'data': {
                'rooms': search_params.get('das[live.rooms]', '1'),
                'area_max': search_params.get('das[live.square][to]', '35'),
                'complex_id': search_params.get('das[map.complex]', '2758'),
                'city': 'almaty',
                'type': 'arenda'
            }
        },
        {
            'url': 'https://m.krisha.kz/api/listings',
            'method': 'POST',
            'data': {
                'rooms': search_params.get('das[live.rooms]', '1'),
                'area_max': search_params.get('das[live.square][to]', '35'),
                'complex_id': search_params.get('das[map.complex]', '2758'),
                'city': 'almaty',
                'type': 'arenda'
            }
        },
        # Alternative API patterns
        {
            'url': 'https://krisha.kz/api/search',
            'method': 'GET',
            'params': {
                'rooms': search_params.get('das[live.rooms]', '1'),
                'area_max': search_params.get('das[live.square][to]', '35'),
                'complex_id': search_params.get('das[map.complex]', '2758'),
                'city': 'almaty',
                'type': 'arenda'
            }
        },
        {
            'url': 'https://krisha.kz/api/listings',
            'method': 'GET',
            'params': {
                'rooms': search_params.get('das[live.rooms]', '1'),
                'area_max': search_params.get('das[live.square][to]', '35'),
                'complex_id': search_params.get('das[map.complex]', '2758'),
                'city': 'almaty',
                'type': 'arenda'
            }
        },
        # JSON API endpoints
        {
            'url': 'https://krisha.kz/api/v1/search',
            'method': 'POST',
            'data': {
                'filters': {
                    'rooms': search_params.get('das[live.rooms]', '1'),
                    'area_max': search_params.get('das[live.square][to]', '35'),
                    'complex_id': search_params.get('das[map.complex]', '2758'),
                    'city': 'almaty',
                    'type': 'arenda'
                }
            }
        },
        # Analytics endpoint (similar to individual flat API)
        {
            'url': 'https://m.krisha.kz/analytics/search',
            'method': 'POST',
            'data': {
                'rooms': search_params.get('das[live.rooms]', '1'),
                'area_max': search_params.get('das[live.square][to]', '35'),
                'complex_id': search_params.get('das[map.complex]', '2758'),
                'city': 'almaty',
                'type': 'arenda'
            }
        },
    ]
    
    for i, test in enumerate(test_endpoints, 1):
        logging.info(f"\nTest {i}: {test['method']} {test['url']}")
        
        try:
            if test['method'] == 'GET':
                response = requests.get(
                    test['url'], 
                    params=test.get('params', {}),
                    headers=headers,
                    timeout=10
                )
            else:
                response = requests.post(
                    test['url'],
                    data=test.get('data', {}),
                    json=test.get('data', {}),
                    headers=headers,
                    timeout=10
                )
            
            result = {
                'url': test['url'],
                'method': test['method'],
                'status_code': response.status_code,
                'content_type': response.headers.get('content-type', ''),
                'content_length': len(response.content),
                'is_json': 'json' in response.headers.get('content-type', '').lower(),
                'response_text': response.text[:500] + '...' if len(response.text) > 500 else response.text
            }
            
            if result['is_json']:
                try:
                    json_data = response.json()
                    result['json_keys'] = list(json_data.keys()) if isinstance(json_data, dict) else []
                    result['json_sample'] = str(json_data)[:300] + '...' if len(str(json_data)) > 300 else str(json_data)
                except json.JSONDecodeError:
                    result['json_error'] = True
            
            results.append(result)
            
            if response.status_code == 200:
                logging.info(f"Status: {response.status_code}")
                if result['is_json']:
                    logging.info(f"   JSON keys: {result.get('json_keys', [])}")
                else:
                    logging.info(f"   Content type: {result['content_type']}")
            else:
                logging.info(f"Status: {response.status_code}")
            
            # Add delay between requests
            time.sleep(1)
            
        except Exception as e:
            logging.info(f"Error: {e}")
            results.append({
                'url': test['url'],
                'method': test['method'],
                'error': str(e)
            })
    
    return results


def test_ajax_endpoints(search_params: Dict) -> List[Dict]:
    """
    Test AJAX endpoints that might be used for dynamic loading.
    
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
        'Referer': 'https://krisha.kz/arenda/kvartiry/almaty/',
        'Origin': 'https://krisha.kz',
    }
    
    # Common AJAX endpoints
    ajax_endpoints = [
        {
            'url': 'https://krisha.kz/ajax/search',
            'method': 'POST',
            'data': {
                'action': 'search',
                'rooms': search_params.get('das[live.rooms]', '1'),
                'area_max': search_params.get('das[live.square][to]', '35'),
                'complex_id': search_params.get('das[map.complex]', '2758'),
                'city': 'almaty',
                'type': 'arenda'
            }
        },
        {
            'url': 'https://krisha.kz/ajax/listings',
            'method': 'POST',
            'data': {
                'action': 'get_listings',
                'filters': json.dumps({
                    'rooms': search_params.get('das[live.rooms]', '1'),
                    'area_max': search_params.get('das[live.square][to]', '35'),
                    'complex_id': search_params.get('das[map.complex]', '2758'),
                    'city': 'almaty',
                    'type': 'arenda'
                })
            }
        },
        {
            'url': 'https://m.krisha.kz/ajax/search',
            'method': 'POST',
            'data': {
                'action': 'search',
                'rooms': search_params.get('das[live.rooms]', '1'),
                'area_max': search_params.get('das[live.square][to]', '35'),
                'complex_id': search_params.get('das[map.complex]', '2758'),
                'city': 'almaty',
                'type': 'arenda'
            }
        },
    ]
    
    for i, test in enumerate(ajax_endpoints, 1):
        logging.info(f"\nAJAX Test {i}: {test['method']} {test['url']}")
        
        try:
            response = requests.post(
                test['url'],
                data=test['data'],
                headers=headers,
                timeout=10
            )
            
            result = {
                'url': test['url'],
                'method': test['method'],
                'status_code': response.status_code,
                'content_type': response.headers.get('content-type', ''),
                'content_length': len(response.content),
                'is_json': 'json' in response.headers.get('content-type', '').lower(),
                'response_text': response.text[:500] + '...' if len(response.text) > 500 else response.text
            }
            
            if result['is_json']:
                try:
                    json_data = response.json()
                    result['json_keys'] = list(json_data.keys()) if isinstance(json_data, dict) else []
                    result['json_sample'] = str(json_data)[:300] + '...' if len(str(json_data)) > 300 else str(json_data)
                except json.JSONDecodeError:
                    result['json_error'] = True
            
            results.append(result)
            
            if response.status_code == 200:
                logging.info(f"Status: {response.status_code}")
                if result['is_json']:
                    logging.info(f"   JSON keys: {result.get('json_keys', [])}")
                else:
                    logging.info(f"   Content type: {result['content_type']}")
            else:
                logging.info(f"Status: {response.status_code}")
            
            time.sleep(1)
            
        except Exception as e:
            logging.info(f"Error: {e}")
            results.append({
                'url': test['url'],
                'method': test['method'],
                'error': str(e)
            })
    
    return results


def analyze_results(results: List[Dict]) -> None:
    """
    Analyze test results and provide recommendations.
    
    :param results: List[Dict], test results
    """
    logging.info(f"\nAnalysis Results:")
    logging.info("=" * 50)
    
    successful_apis = [r for r in results if r.get('status_code') == 200 and r.get('is_json')]
    
    if successful_apis:
        logging.info(f"Found {len(successful_apis)} successful JSON APIs:")
        for api in successful_apis:
            logging.info(f"\n{api['method']} {api['url']}")
            logging.info(f"   JSON keys: {api.get('json_keys', [])}")
            logging.info(f"   Sample: {api.get('json_sample', '')[:100]}...")
    else:
        logging.info("No successful JSON APIs found")
    
    # Check for HTML responses that might contain data
    html_responses = [r for r in results if r.get('status_code') == 200 and 'html' in r.get('content_type', '').lower()]
    
    if html_responses:
        logging.info(f"\nFound {len(html_responses)} HTML responses:")
        for resp in html_responses:
            logging.info(f"   {resp['method']} {resp['url']}")
    
    logging.info(f"\n💡 Recommendations:")
    logging.info("1. Check browser Network tab while loading the search page")
    logging.info("2. Look for XHR/Fetch requests in browser dev tools")
    logging.info("3. Monitor network traffic for AJAX calls")
    logging.info("4. The API might be using a different endpoint structure")


def main():
    """
    Main function to test API endpoints.
    """
    # Search parameters from your URL
    search_params = {
        'das[live.rooms]': '1',
        'das[live.square][to]': '35',
        'das[map.complex]': '2758'
    }
    
    logging.info("Krisha.kz API Testing Tool")
    logging.info("=" * 50)
    logging.info(f"Testing search parameters: {search_params}")
    
    # Test regular API endpoints
    logging.info(f"\nTesting API endpoints...")
    api_results = test_search_api_endpoints(search_params)
    
    # Test AJAX endpoints
    logging.info(f"\nTesting AJAX endpoints...")
    ajax_results = test_ajax_endpoints(search_params)
    
    # Analyze all results
    all_results = api_results + ajax_results
    analyze_results(all_results)
    
    logging.info(f"\n🎯 Next Steps:")
    logging.info("1. Open browser dev tools (F12)")
    logging.info("2. Go to Network tab")
    logging.info("3. Load the search page: https://krisha.kz/arenda/kvartiry/almaty/?das[live.rooms]=1&das[live.square][to]=35&das[map.complex]=2758")
    logging.info("4. Look for XHR/Fetch requests that return JSON data")
    logging.info("5. Copy the request URL and parameters")


if __name__ == "__main__":
    main() 