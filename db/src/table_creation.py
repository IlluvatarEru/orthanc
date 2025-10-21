"""
Table creation module for Krisha.kz database schema.

This module provides functionality to create and manage database tables
for the enhanced flat database system.
"""

import sqlite3
from typing import Optional


class DatabaseSchema:
    """
    Database schema manager for creating and managing database tables.
    
    Provides methods to create tables, indexes, and manage database structure.
    """
    
    def __init__(self, db_path: str = "flats.db"):
        """
        Initialize database schema manager.
        
        :param db_path: str, path to SQLite database file
        """
        self.db_path = db_path
        self.conn = None
    
    def connect(self):
        """
        Establish database connection.
        """
        if self.conn is None:
            self.conn = sqlite3.connect(self.db_path)
            # Enable dict-like access
            self.conn.row_factory = sqlite3.Row
    
    def disconnect(self):
        """
        Close database connection.
        """
        if self.conn:
            self.conn.close()
            self.conn = None
    
    def create_tables(self):
        """
        Create all necessary database tables if they don't exist.
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

        # Create blacklisted JKs table
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS blacklisted_residential_complexes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                complex_id TEXT UNIQUE NOT NULL,
                name TEXT NOT NULL,
                blacklisted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                notes TEXT
            )
        """)
        
        # Create rentals table
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS rental_flats (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                flat_id TEXT NOT NULL,
                price INTEGER NOT NULL,
                area REAL NOT NULL,
                flat_type TEXT CHECK (flat_type IN ('Studio', '1BR', '2BR', '3BR+')),
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
                flat_type TEXT CHECK (flat_type IN ('Studio', '1BR', '2BR', '3BR+')),
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
        
        # Create favorites table (minimal design with JOINs)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS favorites (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                flat_id TEXT NOT NULL,
                flat_type TEXT NOT NULL CHECK (flat_type IN ('rental', 'sale')),
                added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                notes TEXT,
                UNIQUE(flat_id, flat_type)
            )
        """)
        
        # Create blacklisted JKs table
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS blacklisted_jks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                krisha_id TEXT UNIQUE NOT NULL,
                name TEXT NOT NULL,
                blacklisted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                notes TEXT
            )
        """)
        
        # Create mid_prices table for FX data
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS mid_prices (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                currency TEXT NOT NULL,
                rate REAL NOT NULL,
                fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Create JK performance snapshots table
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS jk_performance_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                residential_complex TEXT NOT NULL,
                snapshot_date DATE NOT NULL,
                
                -- Overall statistics
                total_rental_flats INTEGER DEFAULT 0,
                total_sales_flats INTEGER DEFAULT 0,
                
                -- Rental yield statistics
                median_rental_yield REAL,
                mean_rental_yield REAL,
                min_rental_yield REAL,
                max_rental_yield REAL,
                
                -- Rental price per m2 statistics
                min_rent_price_per_m2 REAL,
                max_rent_price_per_m2 REAL,
                mean_rent_price_per_m2 REAL,
                median_rent_price_per_m2 REAL,
                
                -- Sales price per m2 statistics
                min_sales_price_per_m2 REAL,
                max_sales_price_per_m2 REAL,
                mean_sales_price_per_m2 REAL,
                median_sales_price_per_m2 REAL,
                
                -- Studio statistics
                studio_rental_count INTEGER DEFAULT 0,
                studio_sales_count INTEGER DEFAULT 0,
                studio_median_rent_yield REAL,
                studio_mean_rent_yield REAL,
                studio_median_rent_price_per_m2 REAL,
                studio_median_sales_price_per_m2 REAL,
                
                -- 1BR statistics
                onebr_rental_count INTEGER DEFAULT 0,
                onebr_sales_count INTEGER DEFAULT 0,
                onebr_median_rent_yield REAL,
                onebr_mean_rent_yield REAL,
                onebr_median_rent_price_per_m2 REAL,
                onebr_median_sales_price_per_m2 REAL,
                
                -- 2BR statistics
                twobr_rental_count INTEGER DEFAULT 0,
                twobr_sales_count INTEGER DEFAULT 0,
                twobr_median_rent_yield REAL,
                twobr_mean_rent_yield REAL,
                twobr_median_rent_price_per_m2 REAL,
                twobr_median_sales_price_per_m2 REAL,
                
                -- 3BR+ statistics
                threebr_rental_count INTEGER DEFAULT 0,
                threebr_sales_count INTEGER DEFAULT 0,
                threebr_median_rent_yield REAL,
                threebr_mean_rent_yield REAL,
                threebr_median_rent_price_per_m2 REAL,
                threebr_median_sales_price_per_m2 REAL,
                
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(residential_complex, snapshot_date)
            )
        """)
        
        self.conn.commit()
        self.disconnect()
    
    def create_indexes(self):
        """
        Create all necessary database indexes for faster queries.
        """
        self.connect()
        
        # Rental flats indexes
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_rental_flat_id ON rental_flats(flat_id)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_rental_query_date ON rental_flats(query_date)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_rental_price ON rental_flats(price)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_rental_area ON rental_flats(area)")
        
        # Sales flats indexes
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_sales_flat_id ON sales_flats(flat_id)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_sales_query_date ON sales_flats(query_date)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_sales_price ON sales_flats(price)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_sales_area ON sales_flats(area)")
        
        # Residential complexes indexes
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_complex_id ON residential_complexes(complex_id)")
        
        # Favorites indexes
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_favorites_flat_id ON favorites(flat_id)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_favorites_type ON favorites(flat_type)")
        
        # JK performance snapshots indexes
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_jk_snapshots_complex ON jk_performance_snapshots(residential_complex)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_jk_snapshots_date ON jk_performance_snapshots(snapshot_date)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_jk_snapshots_complex_date ON jk_performance_snapshots(residential_complex, snapshot_date)")
        
        # Flat type indexes
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_rental_flat_type ON rental_flats(flat_type)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_sales_flat_type ON sales_flats(flat_type)")
        
        # FX indexes
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_currency_fetched ON mid_prices(currency, fetched_at)")
        
        # Blacklisted JKs indexes
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_blacklisted_krisha_id ON blacklisted_jks(krisha_id)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_blacklisted_name ON blacklisted_jks(name)")
        
        self.conn.commit()
        self.disconnect()
    
    def initialize_database(self):
        """
        Initialize the complete database schema with tables and indexes.
        """
        self.create_tables()
        self.create_indexes()
    
    def drop_tables(self):
        """
        Drop all tables (use with caution - this will delete all data).
        """
        self.connect()
        
        tables = ['favorites', 'sales_flats', 'rental_flats', 'residential_complexes', 'mid_prices']
        
        for table in tables:
            self.conn.execute(f"DROP TABLE IF EXISTS {table}")
        
        self.conn.commit()
        self.disconnect()
    
    def get_table_info(self) -> dict:
        """
        Get information about all tables in the database.
        
        :return: dict, table information
        """
        self.connect()
        
        try:
            # Get table names
            cursor = self.conn.execute("""
                SELECT name FROM sqlite_master 
                WHERE type='table' AND name NOT LIKE 'sqlite_%'
                ORDER BY name
            """)
            tables = [row[0] for row in cursor.fetchall()]
            
            table_info = {}
            for table in tables:
                # Get table schema
                cursor = self.conn.execute(f"PRAGMA table_info({table})")
                columns = [dict(row) for row in cursor.fetchall()]
                
                # Get row count
                cursor = self.conn.execute(f"SELECT COUNT(*) FROM {table}")
                row_count = cursor.fetchone()[0]
                
                table_info[table] = {
                    'columns': columns,
                    'row_count': row_count
                }
            
            return table_info
            
        finally:
            self.disconnect()


def create_database_schema(db_path: str = "flats.db") -> bool:
    """
    Convenience function to create the complete database schema.
    
    :param db_path: str, path to SQLite database file
    :return: bool, True if successful
    """
    schema = DatabaseSchema(db_path)
    schema.initialize_database()
    return True



if __name__ == "__main__":
    """
    Example usage of the table creation module.
    """
    # Create database schema
    schema = DatabaseSchema()
    schema.initialize_database()
    
    # Get table information
    info = schema.get_table_info()
    print("Database Tables:")
    for table_name, table_data in info.items():
        print(f"  {table_name}: {table_data['row_count']} rows")
        for col in table_data['columns']:
            print(f"    - {col['name']} ({col['type']})")
