
"""
Debug script to see what's being fetched from mig.kz
"""

import requests
import re

def debug_mig_fetch():
    """Debug the mig.kz fetching process."""
    try:
        # Fetch the mig.kz page
        response = requests.get('https://mig.kz/', timeout=10)
        response.raise_for_status()
        
        content = response.text
        print("=== MIG.KZ PAGE CONTENT ===")
        print(f"Content length: {len(content)} characters")
        
        # Look for EUR patterns
        print("\n=== SEARCHING FOR EUR PATTERNS ===")
        eur_pattern = r'EUR.*?(\d+\.?\d*)'
        eur_matches = re.findall(eur_pattern, content)
        print(f"EUR matches found: {eur_matches}")
        
        # Look for USD patterns
        print("\n=== SEARCHING FOR USD PATTERNS ===")
        usd_pattern = r'USD.*?(\d+\.?\d*)'
        usd_matches = re.findall(usd_pattern, content)
        print(f"USD matches found: {usd_matches}")
        
        # Look for any currency patterns
        print("\n=== SEARCHING FOR ANY CURRENCY PATTERNS ===")
        currency_pattern = r'(\w{3}).*?(\d+\.?\d*)'
        currency_matches = re.findall(currency_pattern, content)
        print(f"Currency matches found: {currency_matches[:10]}")  # Show first 10
        
        # Look for specific sections that might contain rates
        print("\n=== LOOKING FOR RATE SECTIONS ===")
        if 'курс' in content.lower():
            print("Found 'курс' (rate) in content")
        if 'валют' in content.lower():
            print("Found 'валют' (currency) in content")
            
        # Show a sample of the content around currency mentions
        print("\n=== SAMPLE CONTENT AROUND CURRENCY MENTIONS ===")
        lines = content.split('\n')
        for i, line in enumerate(lines):
            if 'EUR' in line or 'USD' in line or '₸' in line or 'тенге' in line:
                print(f"Line {i}: {line.strip()}")
                
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    debug_mig_fetch() 