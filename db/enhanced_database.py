"""
Enhanced Database module for Krisha.kz with separate tables for rentals and sales.

This module provides functionality to store and retrieve flat data
with historical tracking and residential complex mapping.
"""

import sqlite3
import json
from datetime import datetime
from typing import List, Optional, Dict
from dataclasses import asdict
from common.krisha_scraper import FlatInfo


class EnhancedFlatDatabase:
    """
    Enhanced database manager for storing flat information with historical tracking.
    
    Provides methods to create tables, insert data, and query stored flats
    with separate tables for rentals and sales.
    """
    
    def __init__(self, db_path: str = "flats.db"):
        """
        Initialize database connection.
        
        :param db_path: str, path to SQLite database file
        """
        self.db_path = db_path
        self.conn = None
        self.create_tables()
    
    def connect(self):
        """
        Establish database connection.
        """
        if self.conn is None:
            self.conn = sqlite3.connect(self.db_path)
            self.conn.row_factory = sqlite3.Row  # Enable dict-like access
    
    def disconnect(self):
        """
        Close database connection.
        """
        if self.conn:
            self.conn.close()
            self.conn = None
    
    def create_tables(self):
        """
        Create necessary database tables if they don't exist.
        """
        self.connect()
        
        # Create residential complexes table
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS residential_complexes (
                id INTEGER PRIMARY KEY,
                complex_id TEXT UNIQUE NOT NULL,
                name TEXT NOT NULL,
                city TEXT,
                district TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Create rentals table
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS rental_flats (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                flat_id TEXT NOT NULL,
                price INTEGER NOT NULL,
                area REAL NOT NULL,
                residential_complex TEXT,
                floor INTEGER,
                total_floors INTEGER,
                construction_year INTEGER,
                parking TEXT,
                description TEXT NOT NULL,
                url TEXT NOT NULL,
                query_date DATE NOT NULL,
                scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(flat_id, query_date)
            )
        """)
        
        # Create sales table
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS sales_flats (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                flat_id TEXT NOT NULL,
                price INTEGER NOT NULL,
                area REAL NOT NULL,
                residential_complex TEXT,
                floor INTEGER,
                total_floors INTEGER,
                construction_year INTEGER,
                parking TEXT,
                description TEXT NOT NULL,
                url TEXT NOT NULL,
                query_date DATE NOT NULL,
                scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(flat_id, query_date)
            )
        """)
        
        # Create indexes for faster queries
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_rental_flat_id ON rental_flats(flat_id)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_rental_query_date ON rental_flats(query_date)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_rental_price ON rental_flats(price)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_rental_area ON rental_flats(area)")
        
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_sales_flat_id ON sales_flats(flat_id)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_sales_query_date ON sales_flats(query_date)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_sales_price ON sales_flats(price)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_sales_area ON sales_flats(area)")
        
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_complex_id ON residential_complexes(complex_id)")
        
        self.conn.commit()
        self.disconnect()
    
    def insert_rental_flat(self, flat_info: FlatInfo, url: str, query_date: str) -> bool:
        """
        Insert rental flat information into database.
        
        :param flat_info: FlatInfo, flat information to store
        :param url: str, original URL of the flat
        :param query_date: str, date when the query was made (YYYY-MM-DD)
        :return: bool, True if successful
        """
        self.connect()
        
        try:
            self.conn.execute("""
                INSERT INTO rental_flats (
                    flat_id, price, area, residential_complex, floor, total_floors,
                    construction_year, parking, description, url, query_date
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                flat_info.flat_id,
                flat_info.price,
                flat_info.area,
                flat_info.residential_complex,
                flat_info.floor,
                flat_info.total_floors,
                flat_info.construction_year,
                flat_info.parking,
                flat_info.description,
                url,
                query_date
            ))
            
            self.conn.commit()
            return True
            
        except sqlite3.IntegrityError:
            # Flat already exists for this query date, update instead
            return self.update_rental_flat(flat_info, url, query_date)
        
        finally:
            self.disconnect()
    
    def insert_sales_flat(self, flat_info: FlatInfo, url: str, query_date: str) -> bool:
        """
        Insert sales flat information into database.
        
        :param flat_info: FlatInfo, flat information to store
        :param url: str, original URL of the flat
        :param query_date: str, date when the query was made (YYYY-MM-DD)
        :return: bool, True if successful
        """
        self.connect()
        
        try:
            self.conn.execute("""
                INSERT INTO sales_flats (
                    flat_id, price, area, residential_complex, floor, total_floors,
                    construction_year, parking, description, url, query_date
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                flat_info.flat_id,
                flat_info.price,
                flat_info.area,
                flat_info.residential_complex,
                flat_info.floor,
                flat_info.total_floors,
                flat_info.construction_year,
                flat_info.parking,
                flat_info.description,
                url,
                query_date
            ))
            
            self.conn.commit()
            return True
            
        except sqlite3.IntegrityError:
            # Flat already exists for this query date, update instead
            return self.update_sales_flat(flat_info, url, query_date)
        
        finally:
            self.disconnect()
    
    def update_rental_flat(self, flat_info: FlatInfo, url: str, query_date: str) -> bool:
        """
        Update existing rental flat information.
        
        :param flat_info: FlatInfo, updated flat information
        :param url: str, original URL of the flat
        :param query_date: str, date when the query was made (YYYY-MM-DD)
        :return: bool, True if successful
        """
        self.connect()
        
        try:
            self.conn.execute("""
                UPDATE rental_flats SET
                    price = ?, area = ?, residential_complex = ?, floor = ?,
                    total_floors = ?, construction_year = ?, parking = ?,
                    description = ?, url = ?, updated_at = CURRENT_TIMESTAMP
                WHERE flat_id = ? AND query_date = ?
            """, (
                flat_info.price,
                flat_info.area,
                flat_info.residential_complex,
                flat_info.floor,
                flat_info.total_floors,
                flat_info.construction_year,
                flat_info.parking,
                flat_info.description,
                url,
                flat_info.flat_id,
                query_date
            ))
            
            self.conn.commit()
            return True
            
        finally:
            self.disconnect()
    
    def update_sales_flat(self, flat_info: FlatInfo, url: str, query_date: str) -> bool:
        """
        Update existing sales flat information.
        
        :param flat_info: FlatInfo, updated flat information
        :param url: str, original URL of the flat
        :param query_date: str, date when the query was made (YYYY-MM-DD)
        :return: bool, True if successful
        """
        self.connect()
        
        try:
            self.conn.execute("""
                UPDATE sales_flats SET
                    price = ?, area = ?, residential_complex = ?, floor = ?,
                    total_floors = ?, construction_year = ?, parking = ?,
                    description = ?, url = ?, updated_at = CURRENT_TIMESTAMP
                WHERE flat_id = ? AND query_date = ?
            """, (
                flat_info.price,
                flat_info.area,
                flat_info.residential_complex,
                flat_info.floor,
                flat_info.total_floors,
                flat_info.construction_year,
                flat_info.parking,
                flat_info.description,
                url,
                flat_info.flat_id,
                query_date
            ))
            
            self.conn.commit()
            return True
            
        finally:
            self.disconnect()
    
    def get_rental_flats_by_date(self, query_date: str, limit: Optional[int] = None) -> List[dict]:
        """
        Retrieve rental flats for a specific query date.
        
        :param query_date: str, query date (YYYY-MM-DD)
        :param limit: Optional[int], maximum number of records to return
        :return: List[dict], list of rental flat information
        """
        self.connect()
        
        try:
            query = "SELECT * FROM rental_flats WHERE query_date = ? ORDER BY scraped_at DESC"
            if limit:
                query += f" LIMIT {limit}"
            
            cursor = self.conn.execute(query, (query_date,))
            return [dict(row) for row in cursor.fetchall()]
            
        finally:
            self.disconnect()
    
    def get_sales_flats_by_date(self, query_date: str, limit: Optional[int] = None) -> List[dict]:
        """
        Retrieve sales flats for a specific query date.
        
        :param query_date: str, query date (YYYY-MM-DD)
        :param limit: Optional[int], maximum number of records to return
        :return: List[dict], list of sales flat information
        """
        self.connect()
        
        try:
            query = "SELECT * FROM sales_flats WHERE query_date = ? ORDER BY scraped_at DESC"
            if limit:
                query += f" LIMIT {limit}"
            
            cursor = self.conn.execute(query, (query_date,))
            return [dict(row) for row in cursor.fetchall()]
            
        finally:
            self.disconnect()
    
    def get_historical_statistics(self, start_date: str, end_date: str) -> Dict:
        """
        Get historical statistics for a date range.
        
        :param start_date: str, start date (YYYY-MM-DD)
        :param end_date: str, end date (YYYY-MM-DD)
        :return: Dict, historical statistics
        """
        self.connect()
        
        try:
            # Rental statistics
            cursor = self.conn.execute("""
                SELECT 
                    COUNT(*) as total_rentals,
                    MIN(price) as min_rental_price,
                    MAX(price) as max_rental_price,
                    AVG(price) as avg_rental_price,
                    MIN(area) as min_rental_area,
                    MAX(area) as max_rental_area,
                    AVG(area) as avg_rental_area
                FROM rental_flats 
                WHERE query_date BETWEEN ? AND ?
            """, (start_date, end_date))
            rental_stats = dict(cursor.fetchone())
            
            # Sales statistics
            cursor = self.conn.execute("""
                SELECT 
                    COUNT(*) as total_sales,
                    MIN(price) as min_sales_price,
                    MAX(price) as max_sales_price,
                    AVG(price) as avg_sales_price,
                    MIN(area) as min_sales_area,
                    MAX(area) as max_sales_area,
                    AVG(area) as avg_sales_area
                FROM sales_flats 
                WHERE query_date BETWEEN ? AND ?
            """, (start_date, end_date))
            sales_stats = dict(cursor.fetchone())
            
            # Daily counts
            cursor = self.conn.execute("""
                SELECT query_date, COUNT(*) as count
                FROM rental_flats 
                WHERE query_date BETWEEN ? AND ?
                GROUP BY query_date
                ORDER BY query_date
            """, (start_date, end_date))
            rental_daily = [dict(row) for row in cursor.fetchall()]
            
            cursor = self.conn.execute("""
                SELECT query_date, COUNT(*) as count
                FROM sales_flats 
                WHERE query_date BETWEEN ? AND ?
                GROUP BY query_date
                ORDER BY query_date
            """, (start_date, end_date))
            sales_daily = [dict(row) for row in cursor.fetchall()]
            
            return {
                'rental_stats': rental_stats,
                'sales_stats': sales_stats,
                'rental_daily': rental_daily,
                'sales_daily': sales_daily,
                'date_range': {'start': start_date, 'end': end_date}
            }
            
        finally:
            self.disconnect()
    
    def insert_residential_complex(self, complex_id: str, name: str, city: str = None, district: str = None) -> bool:
        """
        Insert residential complex information.
        
        :param complex_id: str, complex ID from Krisha.kz
        :param name: str, complex name
        :param city: str, city name
        :param district: str, district name
        :return: bool, True if successful
        """
        self.connect()
        
        try:
            self.conn.execute("""
                INSERT OR REPLACE INTO residential_complexes (complex_id, name, city, district)
                VALUES (?, ?, ?, ?)
            """, (complex_id, name, city, district))
            
            self.conn.commit()
            return True
            
        finally:
            self.disconnect()
    
    def get_residential_complex_by_id(self, complex_id: str) -> Optional[dict]:
        """
        Get residential complex by ID.
        
        :param complex_id: str, complex ID
        :return: Optional[dict], complex information
        """
        self.connect()
        
        try:
            cursor = self.conn.execute("""
                SELECT * FROM residential_complexes WHERE complex_id = ?
            """, (complex_id,))
            
            row = cursor.fetchone()
            return dict(row) if row else None
            
        finally:
            self.disconnect()
    
    def get_all_residential_complexes(self) -> List[dict]:
        """
        Get all residential complexes.
        
        :return: List[dict], list of all complexes
        """
        self.connect()
        
        try:
            cursor = self.conn.execute("""
                SELECT * FROM residential_complexes ORDER BY name
            """)
            
            return [dict(row) for row in cursor.fetchall()]
            
        finally:
            self.disconnect()


def save_rental_flat_to_db(flat_info: FlatInfo, url: str, query_date: str, db_path: str = "flats.db") -> bool:
    """
    Convenience function to save rental flat information to database.
    
    :param flat_info: FlatInfo, flat information to save
    :param url: str, original URL of the flat
    :param query_date: str, query date (YYYY-MM-DD)
    :param db_path: str, database file path
    :return: bool, True if successful
    """
    db = EnhancedFlatDatabase(db_path)
    return db.insert_rental_flat(flat_info, url, query_date)


def save_sales_flat_to_db(flat_info: FlatInfo, url: str, query_date: str, db_path: str = "flats.db") -> bool:
    """
    Convenience function to save sales flat information to database.
    
    :param flat_info: FlatInfo, flat information to save
    :param url: str, original URL of the flat
    :param query_date: str, query date (YYYY-MM-DD)
    :param db_path: str, database file path
    :return: bool, True if successful
    """
    db = EnhancedFlatDatabase(db_path)
    return db.insert_sales_flat(flat_info, url, query_date)


def main():
    """
    Example usage of the enhanced database module.
    """
    # Initialize database
    db = EnhancedFlatDatabase()
    
    # Example: Get historical statistics for the last 30 days
    from datetime import datetime, timedelta
    
    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
    
    stats = db.get_historical_statistics(start_date, end_date)
    
    print("Enhanced Database Statistics:")
    print(f"Date range: {start_date} to {end_date}")
    print(f"Total rentals: {stats['rental_stats']['total_rentals']}")
    print(f"Total sales: {stats['sales_stats']['total_sales']}")
    
    if stats['rental_stats']['total_rentals'] > 0:
        print(f"Rental price range: {stats['rental_stats']['min_rental_price']:,} - {stats['rental_stats']['max_rental_price']:,} tenge")
    
    if stats['sales_stats']['total_sales'] > 0:
        print(f"Sales price range: {stats['sales_stats']['min_sales_price']:,} - {stats['sales_stats']['max_sales_price']:,} tenge")


if __name__ == "__main__":
    main() 