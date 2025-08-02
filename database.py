"""
Database module for storing flat information from Krisha.kz.

This module provides functionality to store and retrieve flat data
using SQLite database.
"""

import sqlite3
import json
from datetime import datetime
from typing import List, Optional
from dataclasses import asdict
from krisha_scraper import FlatInfo


class FlatDatabase:
    """
    Database manager for storing flat information.
    
    Provides methods to create tables, insert data, and query stored flats.
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
        
        # Create flats table
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS flats (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                flat_id TEXT UNIQUE NOT NULL,
                price INTEGER NOT NULL,
                area REAL NOT NULL,
                residential_complex TEXT,
                floor INTEGER,
                total_floors INTEGER,
                construction_year INTEGER,
                parking TEXT,
                description TEXT NOT NULL,
                url TEXT NOT NULL,
                scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Create index for faster queries
        self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_flat_id ON flats(flat_id)
        """)
        
        self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_price ON flats(price)
        """)
        
        self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_area ON flats(area)
        """)
        
        self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_scraped_at ON flats(scraped_at)
        """)
        
        self.conn.commit()
        self.disconnect()
    
    def insert_flat(self, flat_info: FlatInfo, url: str) -> bool:
        """
        Insert flat information into database.
        
        :param flat_info: FlatInfo, flat information to store
        :param url: str, original URL of the flat
        :return: bool, True if successful, False if flat already exists
        """
        self.connect()
        
        try:
            self.conn.execute("""
                INSERT INTO flats (
                    flat_id, price, area, residential_complex, floor, total_floors,
                    construction_year, parking, description, url
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                url
            ))
            
            self.conn.commit()
            return True
            
        except sqlite3.IntegrityError:
            # Flat already exists, update instead
            return self.update_flat(flat_info, url)
        
        finally:
            self.disconnect()
    
    def update_flat(self, flat_info: FlatInfo, url: str) -> bool:
        """
        Update existing flat information.
        
        :param flat_info: FlatInfo, updated flat information
        :param url: str, original URL of the flat
        :return: bool, True if successful
        """
        self.connect()
        
        try:
            self.conn.execute("""
                UPDATE flats SET
                    price = ?, area = ?, residential_complex = ?, floor = ?,
                    total_floors = ?, construction_year = ?, parking = ?,
                    description = ?, url = ?, updated_at = CURRENT_TIMESTAMP
                WHERE flat_id = ?
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
                flat_info.flat_id
            ))
            
            self.conn.commit()
            return True
            
        finally:
            self.disconnect()
    
    def get_flat_by_id(self, flat_id: str) -> Optional[dict]:
        """
        Retrieve flat information by flat ID.
        
        :param flat_id: str, flat ID to search for
        :return: Optional[dict], flat information or None if not found
        """
        self.connect()
        
        try:
            cursor = self.conn.execute("""
                SELECT * FROM flats WHERE flat_id = ?
            """, (flat_id,))
            
            row = cursor.fetchone()
            return dict(row) if row else None
            
        finally:
            self.disconnect()
    
    def get_all_flats(self, limit: Optional[int] = None) -> List[dict]:
        """
        Retrieve all flats from database.
        
        :param limit: Optional[int], maximum number of records to return
        :return: List[dict], list of flat information
        """
        self.connect()
        
        try:
            query = "SELECT * FROM flats ORDER BY scraped_at DESC"
            if limit:
                query += f" LIMIT {limit}"
            
            cursor = self.conn.execute(query)
            return [dict(row) for row in cursor.fetchall()]
            
        finally:
            self.disconnect()
    
    def search_flats(self, 
                    min_price: Optional[int] = None,
                    max_price: Optional[int] = None,
                    min_area: Optional[float] = None,
                    max_area: Optional[float] = None,
                    residential_complex: Optional[str] = None,
                    limit: Optional[int] = None) -> List[dict]:
        """
        Search flats with filters.
        
        :param min_price: Optional[int], minimum price filter
        :param max_price: Optional[int], maximum price filter
        :param min_area: Optional[float], minimum area filter
        :param max_area: Optional[float], maximum area filter
        :param residential_complex: Optional[str], residential complex filter
        :param limit: Optional[int], maximum number of records to return
        :return: List[dict], filtered flat information
        """
        self.connect()
        
        try:
            conditions = []
            params = []
            
            if min_price is not None:
                conditions.append("price >= ?")
                params.append(min_price)
            
            if max_price is not None:
                conditions.append("price <= ?")
                params.append(max_price)
            
            if min_area is not None:
                conditions.append("area >= ?")
                params.append(min_area)
            
            if max_area is not None:
                conditions.append("area <= ?")
                params.append(max_area)
            
            if residential_complex:
                conditions.append("residential_complex LIKE ?")
                params.append(f"%{residential_complex}%")
            
            query = "SELECT * FROM flats"
            if conditions:
                query += " WHERE " + " AND ".join(conditions)
            
            query += " ORDER BY scraped_at DESC"
            
            if limit:
                query += f" LIMIT {limit}"
            
            cursor = self.conn.execute(query, params)
            return [dict(row) for row in cursor.fetchall()]
            
        finally:
            self.disconnect()
    
    def get_statistics(self) -> dict:
        """
        Get database statistics.
        
        :return: dict, database statistics
        """
        self.connect()
        
        try:
            # Total flats
            cursor = self.conn.execute("SELECT COUNT(*) FROM flats")
            total_flats = cursor.fetchone()[0]
            
            # Price statistics
            cursor = self.conn.execute("""
                SELECT 
                    MIN(price) as min_price,
                    MAX(price) as max_price,
                    AVG(price) as avg_price,
                    COUNT(*) as count
                FROM flats
            """)
            price_stats = dict(cursor.fetchone())
            
            # Area statistics
            cursor = self.conn.execute("""
                SELECT 
                    MIN(area) as min_area,
                    MAX(area) as max_area,
                    AVG(area) as avg_area
                FROM flats
            """)
            area_stats = dict(cursor.fetchone())
            
            # Recent activity
            cursor = self.conn.execute("""
                SELECT COUNT(*) FROM flats 
                WHERE scraped_at >= datetime('now', '-7 days')
            """)
            recent_flats = cursor.fetchone()[0]
            
            return {
                'total_flats': total_flats,
                'recent_flats': recent_flats,
                'price_stats': price_stats,
                'area_stats': area_stats
            }
            
        finally:
            self.disconnect()
    
    def delete_flat(self, flat_id: str) -> bool:
        """
        Delete flat from database.
        
        :param flat_id: str, flat ID to delete
        :return: bool, True if successful
        """
        self.connect()
        
        try:
            cursor = self.conn.execute("DELETE FROM flats WHERE flat_id = ?", (flat_id,))
            self.conn.commit()
            return cursor.rowcount > 0
            
        finally:
            self.disconnect()


def save_flat_to_db(flat_info: FlatInfo, url: str, db_path: str = "flats.db") -> bool:
    """
    Convenience function to save flat information to database.
    
    :param flat_info: FlatInfo, flat information to save
    :param url: str, original URL of the flat
    :param db_path: str, database file path
    :return: bool, True if successful
    """
    db = FlatDatabase(db_path)
    return db.insert_flat(flat_info, url)


def main():
    """
    Example usage of the database module.
    """
    # Initialize database
    db = FlatDatabase()
    
    # Example: Get statistics
    stats = db.get_statistics()
    print("Database Statistics:")
    print(f"Total flats: {stats['total_flats']}")
    print(f"Recent flats (7 days): {stats['recent_flats']}")
    
    if stats['price_stats']['min_price'] is not None:
        print(f"Price range: {stats['price_stats']['min_price']:,} - {stats['price_stats']['max_price']:,} tenge")
    else:
        print("Price range: No data available")
    
    if stats['area_stats']['min_area'] is not None:
        print(f"Area range: {stats['area_stats']['min_area']:.1f} - {stats['area_stats']['max_area']:.1f} m²")
    else:
        print("Area range: No data available")


if __name__ == "__main__":
    main() 