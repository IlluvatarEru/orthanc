#!/usr/bin/env python3
"""
Quick script to query Meridian flats from database and print flat type and area data.
"""
import sys
import os

# Add the project root to the path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from db.src.write_read_database import OrthancDB

def main():
    """Query Meridian flats and print flat type and area data."""
    # Initialize database connection
    db = OrthancDB()
    
    # Query flats for Meridian Apartments
    residential_complex_name = "Meridian Apartments"
    flats = db.get_flats_for_residential_complex(residential_complex_name)
    
    print(f"Found {len(flats)} flats for {residential_complex_name}")
    print()
    
    # Extract flat type and area data
    data = []
    for flat in flats:
        flat_type = flat.flat_type
        area = flat.area
        data.append((flat_type, area))
    
    # Print the data in the requested format
    print("data = [")
    for flat_type, area in data:
        print(f'    ("{flat_type}", {area}),')
    print("]")
    
    # Also print some summary stats
    print(f"\nSummary:")
    print(f"Total flats: {len(data)}")
    
    # Count by flat type
    flat_type_counts = {}
    for flat_type, area in data:
        flat_type_counts[flat_type] = flat_type_counts.get(flat_type, 0) + 1
    
    for flat_type, count in sorted(flat_type_counts.items()):
        print(f"{flat_type}: {count} flats")

if __name__ == "__main__":
    main()
