"""
Write and read operations for Krisha.kz database.

This module provides functionality to store and retrieve flat data
with historical tracking and residential complex mapping.
"""

import sqlite3
from typing import Optional, List, Dict
import logging
from datetime import datetime
from common.src.flat_info import FlatInfo
from common.src.flat_type import FLAT_TYPE_VALUES
from .table_creation import DatabaseSchema


class OrthancDB:
    """
    Database manager for storing and retrieving flat information.

    Provides methods to insert, update, query, and manage flat data
    with separate tables for rentals and sales.
    """

    def __init__(self, db_path: str = "flats.db"):
        """
        Initialize database connection.

        :param db_path: str, path to SQLite database file
        """
        self.db_path = db_path
        self.conn = None
        # Initialize database schema
        schema = DatabaseSchema(db_path)
        schema.initialize_database()

    def connect(self):
        """
        Establish database connection.
        """
        if self.conn is None:
            self.conn = sqlite3.connect(self.db_path)
            self.conn.row_factory = sqlite3.Row
            logging.info("Connected to DB")
        else:
            logging.info("Already connected to DB")

    def disconnect(self):
        """
        Close database connection.
        """
        if self.conn:
            self.conn.close()
            self.conn = None

    def __enter__(self):
        """Support usage as context manager: with OrthancDB() as db: ..."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Ensure connection is closed when exiting context."""
        self.disconnect()
        return False

    # Rental flats operations
    def insert_rental_flat(
        self,
        flat_info: FlatInfo,
        url: str,
        query_date: str,
        flat_type: str = None,
    ) -> bool:
        """
        Insert rental flat information into database.

        :param flat_info: FlatInfo, flat information to store
        :param url: str, original URL of the flat
        :param query_date: str, date when the query was made (YYYY-MM-DD)
        :param flat_type: str, type of flat ('Studio', '1BR', '2BR', '3BR+') - optional, uses flat_info.flat_type if not provided
        :return: bool, True if successful
        """
        self.connect()
        logging.info(f"Inserting rental flat:{flat_info}")

        # Use flat_type from parameter or from flat_info object
        actual_flat_type = flat_type if flat_type is not None else flat_info.flat_type

        try:
            self.conn.execute(
                """
                INSERT INTO rental_flats (
                    flat_id, price, area, flat_type, residential_complex, floor, total_floors,
                    construction_year, parking, description, url, query_date, archived
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
                (
                    flat_info.flat_id,
                    flat_info.price,
                    flat_info.area,
                    actual_flat_type,
                    flat_info.residential_complex,
                    flat_info.floor,
                    flat_info.total_floors,
                    flat_info.construction_year,
                    flat_info.parking,
                    flat_info.description,
                    url,
                    query_date,
                    1 if flat_info.archived else 0,
                ),
            )

            self.conn.commit()
            return True

        except sqlite3.IntegrityError:
            logging.warning(
                f"Flat {flat_info.flat_id} already exists for {query_date}, updating instead"
            )
            # Flat already exists for this query date, update instead
            return self.update_rental_flat(flat_info, url, query_date, flat_type)
        except Exception as e:
            logging.error(f"Error inserting rental flat {flat_info.flat_id}: {e}")
            return False

    def insert_sales_flat(
        self,
        flat_info: FlatInfo,
        url: str,
        query_date: str,
        flat_type: str = None,
        city: str = None,
    ) -> bool:
        """
        Insert sales flat information into database.

        :param flat_info: FlatInfo, flat information to store
        :param url: str, original URL of the flat
        :param query_date: str, date when the query was made (YYYY-MM-DD)
        :param flat_type: str, type of flat ('Studio', '1BR', '2BR', '3BR+') - optional, uses flat_info.flat_type if not provided
        :param city: str, city name in Cyrillic (e.g. "Алматы")
        :return: bool, True if successful
        """
        self.connect()

        # Use flat_type from parameter or from flat_info object
        actual_flat_type = flat_type if flat_type is not None else flat_info.flat_type

        try:
            self.conn.execute(
                """
                INSERT INTO sales_flats (
                    flat_id, price, area, flat_type, residential_complex, floor, total_floors,
                    construction_year, parking, description, url, query_date, archived, city,
                    published_at, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
                (
                    flat_info.flat_id,
                    flat_info.price,
                    flat_info.area,
                    actual_flat_type,
                    flat_info.residential_complex,
                    flat_info.floor,
                    flat_info.total_floors,
                    flat_info.construction_year,
                    flat_info.parking,
                    flat_info.description,
                    url,
                    query_date,
                    1 if flat_info.archived else 0,
                    city,
                    getattr(flat_info, "published_at", None),
                    getattr(flat_info, "created_at", None),
                ),
            )

            self.conn.commit()
            return True

        except sqlite3.IntegrityError:
            logging.warning(
                f"Flat {flat_info.flat_id} already exists for {query_date}, updating instead"
            )
            # Flat already exists for this query date, update instead
            return self.update_sales_flat(flat_info, url, query_date, flat_type)
        except Exception as e:
            logging.error(f"Error inserting sales flat {flat_info.flat_id}: {e}")
            return False

    def update_rental_flat(
        self,
        flat_info: FlatInfo,
        url: str,
        query_date: str,
        flat_type: str = None,
    ) -> bool:
        """
        Update existing rental flat information.

        :param flat_info: FlatInfo, updated flat information
        :param url: str, original URL of the flat
        :param query_date: str, date when the query was made (YYYY-MM-DD)
        :param flat_type: str, type of flat ('Studio', '1BR', '2BR', '3BR+') - optional, uses flat_info.flat_type if not provided
        :return: bool, True if successful
        """
        self.connect()

        # Use flat_type from parameter or from flat_info object
        actual_flat_type = flat_type if flat_type is not None else flat_info.flat_type

        self.conn.execute(
            """
            UPDATE rental_flats SET
                price = ?, area = ?, flat_type = ?, residential_complex = ?, floor = ?,
                total_floors = ?, construction_year = ?, parking = ?,
                description = ?, url = ?, archived = ?, updated_at = CURRENT_TIMESTAMP
            WHERE flat_id = ? AND query_date = ?
        """,
            (
                flat_info.price,
                flat_info.area,
                actual_flat_type,
                flat_info.residential_complex,
                flat_info.floor,
                flat_info.total_floors,
                flat_info.construction_year,
                flat_info.parking,
                flat_info.description,
                url,
                1 if flat_info.archived else 0,
                flat_info.flat_id,
                query_date,
            ),
        )

        self.conn.commit()
        self.disconnect()
        return True

    def update_sales_flat(
        self,
        flat_info: FlatInfo,
        url: str,
        query_date: str,
        flat_type: str = None,
    ) -> bool:
        """
        Update existing sales flat information.

        :param flat_info: FlatInfo, updated flat information
        :param url: str, original URL of the flat
        :param query_date: str, date when the query was made (YYYY-MM-DD)
        :param flat_type: str, type of flat ('Studio', '1BR', '2BR', '3BR+') - optional, uses flat_info.flat_type if not provided
        :return: bool, True if successful
        """
        self.connect()

        # Use flat_type from parameter or from flat_info object
        actual_flat_type = flat_type if flat_type is not None else flat_info.flat_type

        self.conn.execute(
            """
            UPDATE sales_flats SET
                price = ?, area = ?, flat_type = ?, residential_complex = ?, floor = ?,
                total_floors = ?, construction_year = ?, parking = ?,
                description = ?, url = ?, archived = ?,
                published_at = ?, created_at = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE flat_id = ? AND query_date = ?
        """,
            (
                flat_info.price,
                flat_info.area,
                actual_flat_type,
                flat_info.residential_complex,
                flat_info.floor,
                flat_info.total_floors,
                flat_info.construction_year,
                flat_info.parking,
                flat_info.description,
                url,
                1 if flat_info.archived else 0,
                getattr(flat_info, "published_at", None),
                getattr(flat_info, "created_at", None),
                flat_info.flat_id,
                query_date,
            ),
        )

        self.conn.commit()
        self.disconnect()
        return True

    # Query operations
    def get_rental_flats_by_date(
        self, query_date: str, limit: Optional[int] = None
    ) -> List[FlatInfo]:
        """
        Retrieve rental flats for a specific query date.

        :param query_date: str, query date (YYYY-MM-DD)
        :param limit: Optional[int], maximum number of records to return
        :return: List[FlatInfo], list of rental flat information
        """
        self.connect()

        query = (
            "SELECT * FROM rental_flats WHERE query_date = ? ORDER BY scraped_at DESC"
        )
        if limit:
            query += f" LIMIT {limit}"

        cursor = self.conn.execute(query, (query_date,))
        result = []
        for row in cursor.fetchall():
            flat = FlatInfo(
                flat_id=row["flat_id"],
                price=row["price"],
                area=row["area"],
                flat_type=row["flat_type"],
                residential_complex=row["residential_complex"],
                floor=row["floor"],
                total_floors=row["total_floors"],
                construction_year=row["construction_year"],
                parking=row["parking"],
                description=row["description"] or "",
                is_rental=True,
                archived=bool(row["archived"]),
            )
            result.append(flat)

        self.disconnect()
        return result

    def get_sales_flats_by_date(
        self, query_date: str, limit: Optional[int] = None
    ) -> List[FlatInfo]:
        """
        Retrieve sales flats for a specific query date.

        :param query_date: str, query date (YYYY-MM-DD)
        :param limit: Optional[int], maximum number of records to return
        :return: List[FlatInfo], list of sales flat information
        """
        self.connect()

        query = (
            "SELECT * FROM sales_flats WHERE query_date = ? ORDER BY scraped_at DESC"
        )
        if limit:
            query += f" LIMIT {limit}"

        cursor = self.conn.execute(query, (query_date,))
        result = []
        for row in cursor.fetchall():
            flat = FlatInfo(
                flat_id=row["flat_id"],
                price=row["price"],
                area=row["area"],
                flat_type=row["flat_type"],
                residential_complex=row["residential_complex"],
                floor=row["floor"],
                total_floors=row["total_floors"],
                construction_year=row["construction_year"],
                parking=row["parking"],
                description=row["description"] or "",
                is_rental=False,
                archived=bool(row["archived"]),
            )
            result.append(flat)

        self.disconnect()
        return result

    def get_historical_statistics(
        self, start_date: str, end_date: str, jk: Optional[str] = None
    ) -> Dict:
        """
        Get historical statistics for a date range.

        :param start_date: str, start date (YYYY-MM-DD)
        :param end_date: str, end date (YYYY-MM-DD)
        :param jk: Optional[str], residential complex name to filter by
        :return: Dict, historical statistics
        """
        self.connect()

        # Build WHERE clause for JK filtering
        jk_condition = ""
        if jk:
            jk_condition = " AND residential_complex = ?"

        # Rental statistics
        cursor = self.conn.execute(
            f"""
            SELECT 
                COUNT(*) as total_rentals,
                MIN(price) as min_rental_price,
                MAX(price) as max_rental_price,
                AVG(price) as avg_rental_price,
                MIN(area) as min_rental_area,
                MAX(area) as max_rental_area,
                AVG(area) as avg_rental_area
            FROM rental_flats 
            WHERE query_date BETWEEN ? AND ?{jk_condition}
        """,
            (start_date, end_date) + ((jk,) if jk else ()),
        )
        rental_stats = dict(cursor.fetchone())

        # Sales statistics
        cursor = self.conn.execute(
            f"""
            SELECT 
                COUNT(*) as total_sales,
                MIN(price) as min_sales_price,
                MAX(price) as max_sales_price,
                AVG(price) as avg_sales_price,
                MIN(area) as min_sales_area,
                MAX(area) as max_sales_area,
                AVG(area) as avg_sales_area
            FROM sales_flats 
            WHERE query_date BETWEEN ? AND ?{jk_condition}
        """,
            (start_date, end_date) + ((jk,) if jk else ()),
        )
        sales_stats = dict(cursor.fetchone())

        # Daily counts
        cursor = self.conn.execute(
            f"""
            SELECT query_date, COUNT(*) as count
            FROM rental_flats 
            WHERE query_date BETWEEN ? AND ?{jk_condition}
            GROUP BY query_date
            ORDER BY query_date
        """,
            (start_date, end_date) + ((jk,) if jk else ()),
        )
        rental_daily = [dict(row) for row in cursor.fetchall()]

        cursor = self.conn.execute(
            f"""
            SELECT query_date, COUNT(*) as count
            FROM sales_flats 
            WHERE query_date BETWEEN ? AND ?{jk_condition}
            GROUP BY query_date
            ORDER BY query_date
        """,
            (start_date, end_date) + ((jk,) if jk else ()),
        )
        sales_daily = [dict(row) for row in cursor.fetchall()]

        result = {
            "rental_stats": rental_stats,
            "sales_stats": sales_stats,
            "rental_daily": rental_daily,
            "sales_daily": sales_daily,
            "date_range": {"start": start_date, "end": end_date},
        }

        # Add JK filter info if specified
        if jk:
            result["residential_complex"] = jk

        self.disconnect()
        return result

    # Residential complexes operations
    def insert_residential_complex(
        self, complex_id: str, name: str, city: str = None, district: str = None
    ) -> bool:
        """
        Insert residential complex information.

        :param complex_id: str, complex ID from Krisha.kz
        :param name: str, complex name
        :param city: str, city name
        :param district: str, district name
        :return: bool, True if successful
        """
        self.connect()

        self.conn.execute(
            """
            INSERT OR REPLACE INTO residential_complexes (complex_id, name, city, district)
            VALUES (?, ?, ?, ?)
        """,
            (complex_id, name, city, district),
        )

        self.conn.commit()
        self.disconnect()
        return True

    def get_residential_complex_by_id(self, complex_id: str) -> Optional[dict]:
        """
        Get residential complex by ID.

        :param complex_id: str, complex ID
        :return: Optional[dict], complex information
        """
        self.connect()

        cursor = self.conn.execute(
            """
            SELECT * FROM residential_complexes WHERE complex_id = ?
        """,
            (complex_id,),
        )

        row = cursor.fetchone()
        result = dict(row) if row else None
        self.disconnect()
        return result

    def get_all_residential_complexes(self) -> List[dict]:
        """
        Get all residential complexes.

        :return: List[dict], list of all complexes
        """
        self.connect()

        cursor = self.conn.execute("""
            SELECT * FROM residential_complexes ORDER BY name
        """)

        result = [dict(row) for row in cursor.fetchall()]
        self.disconnect()
        return result

    # Real estate developers operations
    def insert_developer(self, name: str, category: str = "indifferent") -> bool:
        """Insert a real estate developer (ignore if already exists)."""
        self.connect()
        self.conn.execute(
            "INSERT OR IGNORE INTO real_estate_developers (name, category) VALUES (?, ?)",
            (name, category),
        )
        self.conn.commit()
        self.disconnect()
        return True

    def get_developer(self, name: str) -> Optional[dict]:
        """Get developer info by name."""
        self.connect()
        cursor = self.conn.execute(
            "SELECT name, category FROM real_estate_developers WHERE name = ?",
            (name,),
        )
        row = cursor.fetchone()
        result = dict(row) if row else None
        self.disconnect()
        return result

    def get_all_developers(self) -> List[dict]:
        """Get all real estate developers."""
        self.connect()
        cursor = self.conn.execute(
            "SELECT name, category FROM real_estate_developers ORDER BY name"
        )
        result = [dict(row) for row in cursor.fetchall()]
        self.disconnect()
        return result

    def update_developer_category(self, name: str, category: str) -> bool:
        """Update developer category (good/bad/indifferent)."""
        self.connect()
        self.conn.execute(
            "UPDATE real_estate_developers SET category = ? WHERE name = ?",
            (category, name),
        )
        self.conn.commit()
        self.disconnect()
        return True

    def update_residential_complex_developer(
        self, jk_name: str, developer_name: str
    ) -> bool:
        """Set the developer for a residential complex."""
        self.connect()
        self.conn.execute(
            "UPDATE residential_complexes SET developer = ? WHERE name = ?",
            (developer_name, jk_name),
        )
        self.conn.commit()
        self.disconnect()
        return True

    def get_developer_for_jk(self, residential_complex: str) -> Optional[dict]:
        """Get developer info for a JK (join residential_complexes + real_estate_developers)."""
        self.connect()
        cursor = self.conn.execute(
            """SELECT d.name, d.category
               FROM residential_complexes rc
               JOIN real_estate_developers d ON rc.developer = d.name
               WHERE rc.name = ?""",
            (residential_complex,),
        )
        row = cursor.fetchone()
        result = dict(row) if row else None
        self.disconnect()
        return result

    def get_flats_for_residential_complex(
        self, residential_complex: str, sales_or_rentals: str = "both"
    ) -> List[FlatInfo]:
        """
        Get all flats for a specific residential complex.

        :param residential_complex: str, name of the residential complex
        :param sales_or_rentals: str, 'rental', 'sales', or 'both'
        :return: List[FlatInfo], list of flats for the complex
        """
        self.connect()
        flats = []

        if sales_or_rentals in ["rental", "both"]:
            cursor = self.conn.execute(
                """
                SELECT DISTINCT flat_id, price, area, flat_type, residential_complex, floor, 
                       total_floors, construction_year, parking, description, url, 
                       query_date, scraped_at, archived
                FROM rental_flats 
                WHERE residential_complex LIKE ? AND (archived = 0 OR archived IS NULL)
                ORDER BY flat_id, query_date DESC
            """,
                (f"%{residential_complex}%",),
            )

            rental_data = {}
            for row in cursor.fetchall():
                flat_id = row[0]
                if flat_id not in rental_data:
                    rental_data[flat_id] = FlatInfo(
                        flat_id=row[0],
                        price=row[1],
                        area=row[2],
                        flat_type=row[3],  # Use actual flat_type from database
                        residential_complex=row[4],
                        floor=row[5],
                        total_floors=row[6],
                        construction_year=row[7],
                        parking=row[8],
                        description=row[9] or "",
                        is_rental=True,
                        archived=bool(row[13])
                        if len(row) > 13 and row[13] is not None
                        else False,
                    )

            flats.extend(list(rental_data.values()))

        if sales_or_rentals in ["sales", "both"]:
            cursor = self.conn.execute(
                """
                SELECT DISTINCT flat_id, price, area, flat_type, residential_complex, floor, 
                       total_floors, construction_year, parking, description, url, 
                       query_date, scraped_at, archived
                FROM sales_flats 
                WHERE residential_complex LIKE ? AND (archived = 0 OR archived IS NULL)
                ORDER BY flat_id, query_date DESC
            """,
                (f"%{residential_complex}%",),
            )

            sales_data = {}
            for row in cursor.fetchall():
                flat_id = row[0]
                if flat_id not in sales_data:
                    sales_data[flat_id] = FlatInfo(
                        flat_id=row[0],
                        price=row[1],
                        area=row[2],
                        flat_type=row[3],  # Use actual flat_type from database
                        residential_complex=row[4],
                        floor=row[5],
                        total_floors=row[6],
                        construction_year=row[7],
                        parking=row[8],
                        description=row[9] or "",
                        is_rental=False,
                        archived=bool(row[13])
                        if len(row) > 13 and row[13] is not None
                        else False,
                    )

            flats.extend(list(sales_data.values()))

        # Sort by price (rentals first, then sales)
        flats.sort(key=lambda x: (x.is_rental == False, x.price))

        return flats

    def get_flats_by_complex(
        self, residential_complex_name: str, flat_type: str
    ) -> List[FlatInfo]:
        """
        Get flats for a specific complex and type.

        :param residential_complex_name: str, name of the residential complex
        :param flat_type: str, 'rental' or 'sales'
        :return: List[FlatInfo], list of flats for the complex
        """
        if flat_type == "rental":
            return self.get_flats_for_residential_complex(
                residential_complex_name, "rental"
            )
        elif flat_type == "sales":
            return self.get_flats_for_residential_complex(
                residential_complex_name, "sales"
            )
        else:
            raise ValueError(
                f"Invalid flat_type: {flat_type}. Must be 'rental' or 'sales'"
            )

    # Favorites operations
    def add_to_favorites(self, flat_id: str, flat_type: str, notes: str = None) -> bool:
        """
        Add a flat to favorites (minimal design - just store ID and type).

        :param flat_id: str, flat ID to add to favorites
        :param flat_type: str, 'rental' or 'sale'
        :param notes: str, optional notes about the flat
        :return: bool, True if successful
        """
        self.connect()

        self.conn.execute(
            """
            INSERT OR REPLACE INTO favorites (flat_id, flat_type, notes)
            VALUES (?, ?, ?)
        """,
            (flat_id, flat_type, notes),
        )

        self.conn.commit()
        self.disconnect()
        return True

    def remove_from_favorites(self, flat_id: str, flat_type: str) -> bool:
        """
        Remove a flat from favorites.

        :param flat_id: str, flat ID
        :param flat_type: str, 'rental' or 'sale'
        :return: bool, True if successful
        """
        self.connect()

        self.conn.execute(
            """
            DELETE FROM favorites 
            WHERE flat_id = ? AND flat_type = ?
        """,
            (flat_id, flat_type),
        )

        self.conn.commit()
        self.disconnect()
        return True

    def get_favorites(self) -> List[dict]:
        """
        Get all favorites with current flat data using JOINs.
        Only returns the latest entry for each flat_id to avoid duplicates.

        :return: List[dict], list of favorite flats with current data
        """
        self.connect()

        # Get rental favorites with current data (latest query_date only)
        rental_favorites = self.conn.execute("""
            SELECT 
                f.flat_id, f.flat_type, f.added_at, f.notes,
                rf.price, rf.area, rf.residential_complex, rf.floor,
                rf.total_floors, rf.construction_year, rf.parking,
                rf.description, rf.url, rf.scraped_at
            FROM favorites f
            JOIN (
                SELECT rf_inner.*,
                       ROW_NUMBER() OVER (PARTITION BY rf_inner.flat_id ORDER BY rf_inner.query_date DESC) as rn
                FROM rental_flats rf_inner
            ) rf ON f.flat_id = rf.flat_id AND rf.rn = 1
            WHERE f.flat_type = 'rental'
            ORDER BY f.added_at DESC
        """).fetchall()

        # Get sales favorites with current data (latest query_date only)
        sales_favorites = self.conn.execute("""
            SELECT 
                f.flat_id, f.flat_type, f.added_at, f.notes,
                sf.price, sf.area, sf.residential_complex, sf.floor,
                sf.total_floors, sf.construction_year, sf.parking,
                sf.description, sf.url, sf.scraped_at
            FROM favorites f
            JOIN (
                SELECT sf_inner.*,
                       ROW_NUMBER() OVER (PARTITION BY sf_inner.flat_id ORDER BY sf_inner.query_date DESC) as rn
                FROM sales_flats sf_inner
            ) sf ON f.flat_id = sf.flat_id AND sf.rn = 1
            WHERE f.flat_type = 'sale'
            ORDER BY f.added_at DESC
        """).fetchall()

        # Combine and format results
        all_favorites = []
        for row in rental_favorites + sales_favorites:
            all_favorites.append(
                {
                    "flat_id": row[0],
                    "flat_type": row[1],
                    "added_at": row[2],
                    "notes": row[3],
                    "price": row[4],
                    "area": row[5],
                    "residential_complex": row[6],
                    "floor": row[7],
                    "total_floors": row[8],
                    "construction_year": row[9],
                    "parking": row[10],
                    "description": row[11],
                    "url": row[12],
                    "scraped_at": row[13],
                }
            )

        # Sort by added_at descending
        all_favorites.sort(key=lambda x: x["added_at"], reverse=True)

        self.disconnect()
        return all_favorites

    def is_favorite(self, flat_id: str, flat_type: str) -> bool:
        """
        Check if a flat is in favorites.

        :param flat_id: str, flat ID
        :param flat_type: str, 'rental' or 'sale'
        :return: bool, True if flat is in favorites
        """
        self.connect()

        cursor = self.conn.execute(
            """
            SELECT 1 FROM favorites 
            WHERE flat_id = ? AND flat_type = ?
        """,
            (flat_id, flat_type),
        )

        result = cursor.fetchone() is not None
        self.disconnect()
        return result

    # Utility operations
    def get_flat_count(self, flat_type: str) -> int:
        """
        Get count of flats by type.

        :param flat_type: str, 'rental' or 'sales'
        :return: int, count of flats
        """
        self.connect()

        if flat_type == "rental":
            cursor = self.conn.execute(
                "SELECT COUNT(DISTINCT flat_id) FROM rental_flats"
            )
        elif flat_type == "sales":
            cursor = self.conn.execute(
                "SELECT COUNT(DISTINCT flat_id) FROM sales_flats"
            )
        else:
            self.disconnect()
            return 0

        result = cursor.fetchone()
        count = result[0] if result else 0
        self.disconnect()
        return count

    def get_complex_count(self) -> int:
        """
        Get count of residential complexes.

        :return: int, count of complexes
        """
        self.connect()

        cursor = self.conn.execute("SELECT COUNT(*) FROM residential_complexes")
        result = cursor.fetchone()
        count = result[0] if result else 0
        self.disconnect()
        return count

    def move_flat_to_correct_table(self, flat_id: str, correct_type: str) -> bool:
        """
        Move a flat from one table to another if it was incorrectly classified.

        :param flat_id: str, flat ID to move
        :param correct_type: str, 'rental' or 'sales'
        :return: bool, True if successful
        """
        self.connect()

        # Get flat data from the wrong table
        if correct_type == "rental":
            # Move from sales to rental
            cursor = self.conn.execute(
                """
                SELECT flat_id, price, area, residential_complex, floor, total_floors,
                       construction_year, parking, description, url, query_date, archived
                FROM sales_flats 
                WHERE flat_id = ?
            """,
                (flat_id,),
            )
        else:
            # Move from rental to sales
            cursor = self.conn.execute(
                """
                SELECT flat_id, price, area, residential_complex, floor, total_floors,
                       construction_year, parking, description, url, query_date, archived
                FROM rental_flats 
                WHERE flat_id = ?
            """,
                (flat_id,),
            )

        flat_data = cursor.fetchone()
        if not flat_data:
            logging.info(f"Flat {flat_id} not found in source table")
            return False

        # Extract archived status from database (default to False if NULL)
        archived_status = bool(flat_data[11]) if flat_data[11] is not None else False

        # Insert into correct table
        if correct_type == "rental":
            success = self.insert_rental_flat(
                FlatInfo(
                    flat_id=flat_data[0],
                    price=flat_data[1],
                    area=flat_data[2],
                    residential_complex=flat_data[3],
                    floor=flat_data[4],
                    total_floors=flat_data[5],
                    construction_year=flat_data[6],
                    parking=flat_data[7],
                    description=flat_data[8],
                    is_rental=True,
                    archived=archived_status,
                ),
                flat_data[9],  # url
                flat_data[10],  # query_date
            )
            if success:
                # Delete from sales table
                self.conn.execute(
                    "DELETE FROM sales_flats WHERE flat_id = ?", (flat_id,)
                )
        else:
            success = self.insert_sales_flat(
                FlatInfo(
                    flat_id=flat_data[0],
                    price=flat_data[1],
                    area=flat_data[2],
                    residential_complex=flat_data[3],
                    floor=flat_data[4],
                    total_floors=flat_data[5],
                    construction_year=flat_data[6],
                    parking=flat_data[7],
                    description=flat_data[8],
                    is_rental=False,
                    archived=archived_status,
                ),
                flat_data[9],  # url
                flat_data[10],  # query_date
            )
            if success:
                # Delete from rental table
                self.conn.execute(
                    "DELETE FROM rental_flats WHERE flat_id = ?", (flat_id,)
                )

        self.conn.commit()
        self.disconnect()
        return success

    # JK Performance Snapshots
    def create_jk_performance_snapshot(
        self, residential_complex: str, snapshot_date: str
    ) -> bool:
        """
        Create a performance snapshot for a residential complex.

        :param residential_complex: str, name of the residential complex
        :param snapshot_date: str, date of the snapshot (YYYY-MM-DD)
        :return: bool, True if successful
        """
        self.connect()

        # Get rental and sales data for the complex
        rental_data = self.conn.execute(
            """
            SELECT price, area, flat_type, (price * 12.0 / area) as yield_per_m2
            FROM rental_flats 
            WHERE residential_complex = ? AND query_date = ?
        """,
            (residential_complex, snapshot_date),
        ).fetchall()

        sales_data = self.conn.execute(
            """
            SELECT price, area, flat_type, (price / area) as price_per_m2
            FROM sales_flats 
            WHERE residential_complex = ? AND query_date = ?
        """,
            (residential_complex, snapshot_date),
        ).fetchall()

        if not rental_data and not sales_data:
            logging.info(f"No data found for {residential_complex} on {snapshot_date}")
            return False

        # Calculate overall statistics
        total_rental_flats = len(rental_data)
        total_sales_flats = len(sales_data)

        # Rental yield statistics
        rental_yields = [row[3] for row in rental_data if row[3] is not None]
        median_rental_yield = (
            self._calculate_median(rental_yields) if rental_yields else None
        )
        mean_rental_yield = (
            sum(rental_yields) / len(rental_yields) if rental_yields else None
        )
        min_rental_yield = min(rental_yields) if rental_yields else None
        max_rental_yield = max(rental_yields) if rental_yields else None

        # Rental price per m2 statistics
        rent_prices_per_m2 = [row[0] / row[1] for row in rental_data if row[1] > 0]
        min_rent_price_per_m2 = min(rent_prices_per_m2) if rent_prices_per_m2 else None
        max_rent_price_per_m2 = max(rent_prices_per_m2) if rent_prices_per_m2 else None
        mean_rent_price_per_m2 = (
            sum(rent_prices_per_m2) / len(rent_prices_per_m2)
            if rent_prices_per_m2
            else None
        )
        median_rent_price_per_m2 = (
            self._calculate_median(rent_prices_per_m2) if rent_prices_per_m2 else None
        )

        # Sales price per m2 statistics
        sales_prices_per_m2 = [row[0] / row[1] for row in sales_data if row[1] > 0]
        min_sales_price_per_m2 = (
            min(sales_prices_per_m2) if sales_prices_per_m2 else None
        )
        max_sales_price_per_m2 = (
            max(sales_prices_per_m2) if sales_prices_per_m2 else None
        )
        mean_sales_price_per_m2 = (
            sum(sales_prices_per_m2) / len(sales_prices_per_m2)
            if sales_prices_per_m2
            else None
        )
        median_sales_price_per_m2 = (
            self._calculate_median(sales_prices_per_m2) if sales_prices_per_m2 else None
        )

        # Calculate statistics by flat type
        flat_types = FLAT_TYPE_VALUES
        type_stats = {}

        for flat_type in flat_types:
            rental_type_data = [row for row in rental_data if row[2] == flat_type]
            sales_type_data = [row for row in sales_data if row[2] == flat_type]

            type_stats[flat_type] = {
                "rental_count": len(rental_type_data),
                "sales_count": len(sales_type_data),
                "rental_yields": [
                    row[3] for row in rental_type_data if row[3] is not None
                ],
                "rental_prices_per_m2": [
                    row[0] / row[1] for row in rental_type_data if row[1] > 0
                ],
                "sales_prices_per_m2": [
                    row[0] / row[1] for row in sales_type_data if row[1] > 0
                ],
            }

        # Insert snapshot
        self.conn.execute(
            """
            INSERT OR REPLACE INTO jk_performance_snapshots (
                residential_complex, snapshot_date, total_rental_flats, total_sales_flats,
                median_rental_yield, mean_rental_yield, min_rental_yield, max_rental_yield,
                min_rent_price_per_m2, max_rent_price_per_m2, mean_rent_price_per_m2, median_rent_price_per_m2,
                min_sales_price_per_m2, max_sales_price_per_m2, mean_sales_price_per_m2, median_sales_price_per_m2,
                studio_rental_count, studio_sales_count, studio_median_rent_yield, studio_mean_rent_yield,
                studio_median_rent_price_per_m2, studio_median_sales_price_per_m2,
                onebr_rental_count, onebr_sales_count, onebr_median_rent_yield, onebr_mean_rent_yield,
                onebr_median_rent_price_per_m2, onebr_median_sales_price_per_m2,
                twobr_rental_count, twobr_sales_count, twobr_median_rent_yield, twobr_mean_rent_yield,
                twobr_median_rent_price_per_m2, twobr_median_sales_price_per_m2,
                threebr_rental_count, threebr_sales_count, threebr_median_rent_yield, threebr_mean_rent_yield,
                threebr_median_rent_price_per_m2, threebr_median_sales_price_per_m2
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
            (
                residential_complex,
                snapshot_date,
                total_rental_flats,
                total_sales_flats,
                median_rental_yield,
                mean_rental_yield,
                min_rental_yield,
                max_rental_yield,
                min_rent_price_per_m2,
                max_rent_price_per_m2,
                mean_rent_price_per_m2,
                median_rent_price_per_m2,
                min_sales_price_per_m2,
                max_sales_price_per_m2,
                mean_sales_price_per_m2,
                median_sales_price_per_m2,
                type_stats["Studio"]["rental_count"],
                type_stats["Studio"]["sales_count"],
                self._calculate_median(type_stats["Studio"]["rental_yields"]),
                sum(type_stats["Studio"]["rental_yields"])
                / len(type_stats["Studio"]["rental_yields"])
                if type_stats["Studio"]["rental_yields"]
                else None,
                self._calculate_median(type_stats["Studio"]["rental_prices_per_m2"]),
                self._calculate_median(type_stats["Studio"]["sales_prices_per_m2"]),
                type_stats["1BR"]["rental_count"],
                type_stats["1BR"]["sales_count"],
                self._calculate_median(type_stats["1BR"]["rental_yields"]),
                sum(type_stats["1BR"]["rental_yields"])
                / len(type_stats["1BR"]["rental_yields"])
                if type_stats["1BR"]["rental_yields"]
                else None,
                self._calculate_median(type_stats["1BR"]["rental_prices_per_m2"]),
                self._calculate_median(type_stats["1BR"]["sales_prices_per_m2"]),
                type_stats["2BR"]["rental_count"],
                type_stats["2BR"]["sales_count"],
                self._calculate_median(type_stats["2BR"]["rental_yields"]),
                sum(type_stats["2BR"]["rental_yields"])
                / len(type_stats["2BR"]["rental_yields"])
                if type_stats["2BR"]["rental_yields"]
                else None,
                self._calculate_median(type_stats["2BR"]["rental_prices_per_m2"]),
                self._calculate_median(type_stats["2BR"]["sales_prices_per_m2"]),
                type_stats["3BR+"]["rental_count"],
                type_stats["3BR+"]["sales_count"],
                self._calculate_median(type_stats["3BR+"]["rental_yields"]),
                sum(type_stats["3BR+"]["rental_yields"])
                / len(type_stats["3BR+"]["rental_yields"])
                if type_stats["3BR+"]["rental_yields"]
                else None,
                self._calculate_median(type_stats["3BR+"]["rental_prices_per_m2"]),
                self._calculate_median(type_stats["3BR+"]["sales_prices_per_m2"]),
            ),
        )

        self.conn.commit()
        logging.info(
            f"Created performance snapshot for {residential_complex} on {snapshot_date}"
        )
        return True

    def get_jk_performance_snapshots(
        self,
        residential_complex: str = None,
        start_date: str = None,
        end_date: str = None,
    ) -> List[dict]:
        """
        Get JK performance snapshots.

        :param residential_complex: str, filter by complex name
        :param start_date: str, filter by start date (YYYY-MM-DD)
        :param end_date: str, filter by end date (YYYY-MM-DD)
        :return: List[dict], list of performance snapshots
        """
        self.connect()

        conditions = []
        params = []

        if residential_complex:
            conditions.append("residential_complex = ?")
            params.append(residential_complex)

        if start_date:
            conditions.append("snapshot_date >= ?")
            params.append(start_date)

        if end_date:
            conditions.append("snapshot_date <= ?")
            params.append(end_date)

        query = "SELECT * FROM jk_performance_snapshots"
        if conditions:
            query += " WHERE " + " AND ".join(conditions)

        query += " ORDER BY snapshot_date DESC"

        cursor = self.conn.execute(query, params)
        result = [dict(row) for row in cursor.fetchall()]
        self.disconnect()
        return result

    # FX Operations
    def create_mid_prices_table(self):
        """
        Create mid_prices table if it doesn't exist.
        """
        self.connect()

        if self.conn is None:
            logging.error(
                "Cannot create mid_prices table - database connection is None"
            )
            return

        try:
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS mid_prices (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    currency TEXT NOT NULL,
                    rate REAL NOT NULL,
                    fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # Create index for faster queries
            self.conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_currency_fetched ON mid_prices(currency, fetched_at)
            """)

            self.conn.commit()
            logging.info("mid_prices table created successfully")

        except Exception as e:
            logging.error(f"Error creating mid_prices table: {e}")
            raise

    def insert_exchange_rate(
        self, currency: str, rate: float, fetched_at: datetime = None
    ) -> bool:
        """
        Insert exchange rate into database.

        :param currency: str, currency code (EUR, USD, etc.)
        :param rate: float, exchange rate
        :param fetched_at: datetime, timestamp when rate was fetched
        :return: bool, True if successful
        """
        self.connect()

        if fetched_at is None:
            fetched_at = datetime.now()

        self.conn.execute(
            """
            INSERT INTO mid_prices (currency, rate, fetched_at)
            VALUES (?, ?, ?)
        """,
            (currency, rate, fetched_at),
        )

        self.conn.commit()
        self.disconnect()
        return True

    def get_latest_rate(self, currency: str) -> Optional[float]:
        """
        Get the latest exchange rate for a currency.

        :param currency: str, currency code (EUR or USD)
        :return: Optional[float], latest rate or None if not found
        """
        self.connect()

        cursor = self.conn.execute(
            """
            SELECT rate FROM mid_prices 
            WHERE currency = ? 
            ORDER BY fetched_at DESC 
            LIMIT 1
        """,
            (currency,),
        )

        result = cursor.fetchone()
        rate = float(result["rate"]) if result else None
        self.disconnect()
        return rate

    def get_rates_by_date_range(
        self, currency: str, start_date: str, end_date: str
    ) -> list:
        """
        Get exchange rates for a currency within a date range.

        :param currency: str, currency code
        :param start_date: str, start date (YYYY-MM-DD)
        :param end_date: str, end date (YYYY-MM-DD)
        :return: list, list of rate records
        """
        self.connect()

        cursor = self.conn.execute(
            """
            SELECT currency, rate, fetched_at 
            FROM mid_prices 
            WHERE currency = ? AND DATE(fetched_at) BETWEEN ? AND ?
            ORDER BY fetched_at DESC
        """,
            (currency, start_date, end_date),
        )

        result = [dict(row) for row in cursor.fetchall()]
        self.disconnect()
        return result

    def get_all_currencies(self) -> list:
        """
        Get all unique currencies in the database.

        :return: list, list of currency codes
        """
        self.connect()

        cursor = self.conn.execute("""
            SELECT DISTINCT currency FROM mid_prices ORDER BY currency
        """)

        result = [row["currency"] for row in cursor.fetchall()]
        self.disconnect()
        return result

    def delete_rate_at_timestamp(self, timestamp: datetime) -> int:
        """
        Delete exchange rates at a specific timestamp.

        :param timestamp: datetime, timestamp to delete rates for
        :return: int, number of records deleted
        """
        self.connect()

        cursor = self.conn.execute(
            """
            DELETE FROM mid_prices 
            WHERE fetched_at = ?
        """,
            (timestamp,),
        )

        deleted_count = cursor.rowcount
        self.conn.commit()
        self.disconnect()
        return deleted_count

    # Blacklist operations
    def blacklist_jk(self, krisha_id: str, name: str, notes: str = None) -> bool:
        """
        Add a JK to the blacklist.

        :param krisha_id: str, Krisha ID of the JK
        :param name: str, name of the JK
        :param notes: str, optional notes about why it's blacklisted
        :return: bool, True if successful
        """
        self.connect()

        try:
            self.conn.execute(
                """
                INSERT OR REPLACE INTO blacklisted_jks (krisha_id, name, notes)
                VALUES (?, ?, ?)
            """,
                (krisha_id, name, notes),
            )

            self.conn.commit()
            logging.info(f"Blacklisted JK: {name} (ID: {krisha_id})")
            return True

        except Exception as e:
            logging.error(f"Error blacklisting JK {name}: {e}")
            return False
        finally:
            self.disconnect()

    def blacklist_jks(self, jk_list: List[Dict]) -> int:
        """
        Add multiple JKs to the blacklist.

        :param jk_list: List[Dict], list of JK dictionaries with 'krisha_id', 'name', and optional 'notes'
        :return: int, number of JKs successfully blacklisted
        """
        self.connect()

        success_count = 0

        try:
            for jk in jk_list:
                krisha_id = jk.get("krisha_id")
                name = jk.get("name")
                notes = jk.get("notes")

                if not krisha_id or not name:
                    logging.warning(f"Skipping JK with missing required fields: {jk}")
                    continue

                self.conn.execute(
                    """
                    INSERT OR REPLACE INTO blacklisted_jks (krisha_id, name, notes)
                    VALUES (?, ?, ?)
                """,
                    (krisha_id, name, notes),
                )

                success_count += 1
                logging.info(f"Blacklisted JK: {name} (ID: {krisha_id})")

            self.conn.commit()
            logging.info(f"Successfully blacklisted {success_count} JKs")

        except Exception as e:
            logging.error(f"Error blacklisting JKs: {e}")
        finally:
            self.disconnect()

        return success_count

    def whitelist_jk(self, krisha_id: str = None, name: str = None) -> bool:
        """
        Remove a JK from the blacklist.

        :param krisha_id: str, Krisha ID of the JK to whitelist
        :param name: str, name of the JK to whitelist
        :return: bool, True if successful
        """
        self.connect()

        if not krisha_id and not name:
            logging.error("Either krisha_id or name must be provided")
            return False

        try:
            if krisha_id:
                cursor = self.conn.execute(
                    """
                    DELETE FROM blacklisted_jks WHERE krisha_id = ?
                """,
                    (krisha_id,),
                )
            else:
                cursor = self.conn.execute(
                    """
                    DELETE FROM blacklisted_jks WHERE name = ?
                """,
                    (name,),
                )

            deleted_count = cursor.rowcount
            self.conn.commit()

            if deleted_count > 0:
                logging.info(f"Whitelisted JK: {name or krisha_id}")
                return True
            else:
                logging.warning(f"JK not found in blacklist: {name or krisha_id}")
                return False

        except Exception as e:
            logging.error(f"Error whitelisting JK {name or krisha_id}: {e}")
            return False
        finally:
            self.disconnect()

    def whitelist_jk_by_name(self, name: str) -> bool:
        """
        Remove a JK from the blacklist by name, automatically finding the Krisha ID from the database.

        :param name: str, name of the JK to whitelist
        :return: bool, True if successful
        """
        self.connect()

        # Find the JK in the residential_complexes table
        cursor = self.conn.execute(
            """
            SELECT complex_id FROM residential_complexes WHERE name = ?
        """,
            (name,),
        )

        complex_row = cursor.fetchone()
        if complex_row:
            krisha_id = complex_row[0]
            logging.info(f"Found JK '{name}' with Krisha ID: {krisha_id}")
            self.disconnect()
            return self.whitelist_jk(krisha_id=krisha_id, name=name)
        else:
            self.disconnect()
            raise Exception(f"JK '{name}' not found in residential_complexes table")

    def whitelist_jk_by_id(self, jk_id: str) -> bool:
        """
        Remove a JK from the blacklist by ID, automatically finding the name from the database.

        :param jk_id: str, ID of the JK to whitelist
        :return: bool, True if successful
        """
        self.connect()

        # Find the JK in the residential_complexes table
        cursor = self.conn.execute(
            """
            SELECT name FROM residential_complexes WHERE complex_id = ?
        """,
            (jk_id,),
        )

        complex_row = cursor.fetchone()
        if complex_row:
            name = complex_row[0]
            logging.info(f"Found JK with ID '{jk_id}' and name: {name}")
            self.disconnect()
            return self.whitelist_jk(krisha_id=jk_id, name=name)
        else:
            self.disconnect()
            raise Exception(
                f"JK with ID '{jk_id}' not found in residential_complexes table"
            )

    def whitelist_jks(self, jk_list: List[Dict]) -> int:
        """
        Remove multiple JKs from the blacklist.

        :param jk_list: List[Dict], list of JK dictionaries with 'krisha_id' or 'name'
        :return: int, number of JKs successfully whitelisted
        """
        self.connect()

        success_count = 0

        try:
            for jk in jk_list:
                krisha_id = jk.get("krisha_id")
                name = jk.get("name")

                if not krisha_id and not name:
                    logging.warning(f"Skipping JK with missing identifier: {jk}")
                    continue

                if krisha_id:
                    cursor = self.conn.execute(
                        """
                        DELETE FROM blacklisted_jks WHERE krisha_id = ?
                    """,
                        (krisha_id,),
                    )
                else:
                    cursor = self.conn.execute(
                        """
                        DELETE FROM blacklisted_jks WHERE name = ?
                    """,
                        (name,),
                    )

                if cursor.rowcount > 0:
                    success_count += 1
                    logging.info(f"Whitelisted JK: {name or krisha_id}")
                else:
                    logging.warning(f"JK not found in blacklist: {name or krisha_id}")

            self.conn.commit()
            logging.info(f"Successfully whitelisted {success_count} JKs")

        except Exception as e:
            logging.error(f"Error whitelisting JKs: {e}")
        finally:
            self.disconnect()

        return success_count

    def get_blacklisted_jks(self) -> List[dict]:
        """
        Get all blacklisted JKs.

        :return: List[dict], list of blacklisted JK information
        """
        self.connect()

        cursor = self.conn.execute("""
            SELECT * FROM blacklisted_jks ORDER BY blacklisted_at DESC
        """)

        result = [dict(row) for row in cursor.fetchall()]
        self.disconnect()
        return result

    def is_jk_blacklisted(self, krisha_id: str = None, name: str = None) -> bool:
        """
        Check if a JK is blacklisted.

        :param krisha_id: str, Krisha ID to check
        :param name: str, JK name to check
        :return: bool, True if JK is blacklisted
        """
        self.connect()

        if not krisha_id and not name:
            return False

        try:
            if krisha_id:
                cursor = self.conn.execute(
                    """
                    SELECT 1 FROM blacklisted_jks WHERE krisha_id = ?
                """,
                    (krisha_id,),
                )
            else:
                cursor = self.conn.execute(
                    """
                    SELECT 1 FROM blacklisted_jks WHERE name = ?
                """,
                    (name,),
                )

            result = cursor.fetchone() is not None
            return result

        except Exception as e:
            logging.error(f"Error checking if JK is blacklisted: {e}")
            return False
        finally:
            self.disconnect()

    def blacklist_jk_by_name(self, name: str, notes: str = "") -> bool:
        """
        Blacklist a JK by its name, automatically finding the Krisha ID from the database.

        :param name: str, name of the JK to blacklist
        :param notes: str, optional notes about why it's blacklisted
        :return: bool, True if successful
        """
        self.connect()

        # Find the JK in the residential_complexes table
        cursor = self.conn.execute(
            """
            SELECT complex_id FROM residential_complexes WHERE name = ?
        """,
            (name,),
        )

        complex_row = cursor.fetchone()
        if complex_row:
            krisha_id = complex_row[0]
            logging.info(f"Found JK '{name}' with Krisha ID: {krisha_id}")
            self.disconnect()
            return self.blacklist_jk(krisha_id, name, notes)
        else:
            self.disconnect()
            raise Exception(f"JK '{name}' not found in residential_complexes table")

    def blacklist_jk_by_id(self, jk_id: str, notes: str = "") -> bool:
        """
        Blacklist a JK by its ID, automatically finding the name from the database.

        :param jk_id: str, ID of the JK to blacklist
        :param notes: str, optional notes about why it's blacklisted
        :return: bool, True if successful
        """
        self.connect()

        # Find the JK in the residential_complexes table
        cursor = self.conn.execute(
            """
            SELECT name FROM residential_complexes WHERE complex_id = ?
        """,
            (jk_id,),
        )

        complex_row = cursor.fetchone()
        if complex_row:
            name = complex_row[0]
            logging.info(f"Found JK with ID '{jk_id}' and name: {name}")
            self.disconnect()
            return self.blacklist_jk(jk_id, name, notes)
        else:
            self.disconnect()
            raise Exception(f"JK '{jk_id}' not found in residential_complexes table")

    # ---- District Blacklist Methods ----

    def add_blacklisted_district(
        self, city: str, district: str, notes: str = None
    ) -> bool:
        """Add a district to the blacklist."""
        self.connect()
        try:
            self.conn.execute(
                "INSERT OR IGNORE INTO blacklisted_districts (city, district, notes) VALUES (?, ?, ?)",
                (city, district, notes),
            )
            self.conn.commit()
            return True
        except Exception as e:
            logging.error(f"Error blacklisting district: {e}")
            return False
        finally:
            self.disconnect()

    def remove_blacklisted_district(self, city: str, district: str) -> bool:
        """Remove a district from the blacklist."""
        self.connect()
        try:
            self.conn.execute(
                "DELETE FROM blacklisted_districts WHERE city = ? AND district = ?",
                (city, district),
            )
            self.conn.commit()
            return True
        except Exception as e:
            logging.error(f"Error removing blacklisted district: {e}")
            return False
        finally:
            self.disconnect()

    def get_blacklisted_districts(self, city: str = None) -> List[dict]:
        """Get blacklisted districts, optionally filtered by city."""
        self.connect()
        try:
            if city:
                cursor = self.conn.execute(
                    "SELECT * FROM blacklisted_districts WHERE city = ? ORDER BY district",
                    (city,),
                )
            else:
                cursor = self.conn.execute(
                    "SELECT * FROM blacklisted_districts ORDER BY city, district"
                )
            return [dict(row) for row in cursor.fetchall()]
        finally:
            self.disconnect()

    def get_districts_for_city(self, city: str) -> List[str]:
        """Get all known districts for a city from residential_complexes."""
        self.connect()
        try:
            cursor = self.conn.execute(
                "SELECT DISTINCT district FROM residential_complexes WHERE city = ? AND district IS NOT NULL AND district != '' ORDER BY district",
                (city,),
            )
            return [row[0] for row in cursor.fetchall()]
        finally:
            self.disconnect()

    def find_jk(self, query: str) -> List[dict]:
        """
        Fuzzy-search JKs by name (case-insensitive substring match).

        :param query: str, search term
        :return: List[dict] with keys: name, city, district
        """
        self.connect()
        try:
            cursor = self.conn.execute(
                "SELECT name, city, district FROM residential_complexes WHERE name LIKE ? ORDER BY name",
                (f"%{query}%",),
            )
            return [dict(row) for row in cursor.fetchall()]
        finally:
            self.disconnect()

    def get_jks_in_district(self, city: str, district: str) -> List[dict]:
        """
        Get all JKs in a given district.

        :param city: str, city name (e.g. "Алматы")
        :param district: str, district name (e.g. "Бостандыкский р-н")
        :return: List[dict] with keys: name, city, district
        """
        self.connect()
        try:
            cursor = self.conn.execute(
                "SELECT name, city, district FROM residential_complexes WHERE city = ? AND district = ? ORDER BY name",
                (city, district),
            )
            return [dict(row) for row in cursor.fetchall()]
        finally:
            self.disconnect()

    def get_latest_sales_by_jk(self, jk_name: str) -> List[dict]:
        """
        Get sales data for a residential complex where query_date is within the past 24 hours.

        :param jk_name: str, name of the residential complex
        :return: List[dict], list of recent sales data
        """
        self.connect()

        recent_sales_query = """
            SELECT sf.*
            FROM sales_flats sf
            WHERE sf.residential_complex = ? 
            AND DATE(sf.query_date) >= DATE('now', '-1 day')
            AND (sf.archived = 0 OR sf.archived IS NULL)
        """

        cursor = self.conn.execute(recent_sales_query, (jk_name,))
        recent_sales = [dict(row) for row in cursor.fetchall()]

        self.disconnect()
        return recent_sales

    def get_latest_rentals_for_jk(self, jk_name: str) -> List[dict]:
        """
        Get latest rental data for a residential complex (most recent query_date for each flat_id).

        :param jk_name: str, name of the residential complex
        :return: List[dict], list of latest rental data with all fields
        """
        self.connect()

        query = """
            SELECT rf.*, 
                   ROW_NUMBER() OVER (PARTITION BY rf.flat_id ORDER BY rf.query_date DESC) as rn
            FROM rental_flats rf
            WHERE rf.residential_complex = ? AND (rf.archived = 0 OR rf.archived IS NULL)
        """

        cursor = self.conn.execute(query, (jk_name,))
        latest_rentals = [dict(row) for row in cursor.fetchall() if row["rn"] == 1]

        self.disconnect()
        return latest_rentals

    def get_latest_sales_for_jk(self, jk_name: str, city: str = None) -> List[dict]:
        """
        Get latest sales data for a residential complex (most recent query_date for each flat_id).

        :param jk_name: str, name of the residential complex
        :param city: str, city name in Cyrillic to filter by (optional)
        :return: List[dict], list of latest sales data with all fields
        """
        self.connect()

        params = [jk_name]
        city_filter = ""
        if city:
            city_filter = "AND sf.city = ?"
            params.append(city)

        query = f"""
            SELECT sf.*,
                   ROW_NUMBER() OVER (PARTITION BY sf.flat_id ORDER BY sf.query_date DESC) as rn
            FROM sales_flats sf
            WHERE sf.residential_complex = ? AND (sf.archived = 0 OR sf.archived IS NULL)
            {city_filter}
        """

        cursor = self.conn.execute(query, params)
        latest_sales = [dict(row) for row in cursor.fetchall() if row["rn"] == 1]

        self.disconnect()
        return latest_sales

    def get_historical_rental_stats_by_jk(self, jk_name: str) -> List[dict]:
        """
        Get historical rental statistics grouped by date and flat type for a residential complex.

        :param jk_name: str, name of the residential complex
        :return: List[dict], historical rental statistics with date, flat_type, count, mean_rental, min_rental, max_rental, residential_complex
        """
        self.connect()

        query = """
            SELECT 
                DATE(rf.query_date) as date,
                rf.flat_type,
                COUNT(*) as count,
                AVG(rf.price) as mean_rental,
                MIN(rf.price) as min_rental,
                MAX(rf.price) as max_rental,
                rf.residential_complex
            FROM rental_flats rf
            WHERE rf.residential_complex = ?
            GROUP BY DATE(rf.query_date), rf.flat_type
            ORDER BY date DESC, rf.flat_type
        """

        cursor = self.conn.execute(query, (jk_name,))
        historical_data = [dict(row) for row in cursor.fetchall()]

        self.disconnect()
        return historical_data

    def get_jk_rentals_summary_stats(self, jk_name: str) -> dict:
        """
        Get rental summary statistics for a residential complex.

        :param jk_name: str, name of the residential complex
        :return: dict, with keys: total_rentals, earliest, latest
        """
        self.connect()

        cursor = self.conn.execute(
            """
            SELECT 
                COUNT(*) as total_rentals,
                MIN(query_date) as earliest,
                MAX(query_date) as latest
            FROM rental_flats 
            WHERE residential_complex = ?
        """,
            (jk_name,),
        )

        stats = dict(cursor.fetchone())
        self.disconnect()
        return stats

    def get_jk_rentals_flat_type_distribution(self, jk_name: str) -> dict:
        """
        Get flat type distribution for rentals in a residential complex.

        :param jk_name: str, name of the residential complex
        :return: dict, flat type to count mapping
        """
        self.connect()

        cursor = self.conn.execute(
            """
            SELECT 
                flat_type,
                COUNT(*) as count
            FROM rental_flats 
            WHERE residential_complex = ?
            GROUP BY flat_type
            ORDER BY count DESC
        """,
            (jk_name,),
        )

        flat_type_distribution = {row[0]: row[1] for row in cursor.fetchall()}
        self.disconnect()
        return flat_type_distribution

    def get_rentals_by_date_and_flat_type(
        self, jk_name: str, date: str, flat_type: str
    ) -> List[dict]:
        """
        Get rental flats for a specific date and flat type in a residential complex.

        :param jk_name: str, name of the residential complex
        :param date: str, query date (YYYY-MM-DD)
        :param flat_type: str, flat type (Studio, 1BR, 2BR, 3BR+)
        :return: List[dict], list of rental flats
        """
        self.connect()

        query = """
            SELECT * FROM rental_flats
            WHERE residential_complex = ? 
            AND DATE(query_date) = DATE(?)
            AND flat_type = ?
        """

        cursor = self.conn.execute(query, (jk_name, date, flat_type))
        rentals = [dict(row) for row in cursor.fetchall()]

        self.disconnect()
        return rentals

    def get_sales_by_date_range(
        self, jk_name: str, start_date: str, end_date: str
    ) -> List[dict]:
        """
        Get sales flats for a date range in a residential complex.

        :param jk_name: str, name of the residential complex
        :param start_date: str, start date (YYYY-MM-DD)
        :param end_date: str, end date (YYYY-MM-DD)
        :return: List[dict], list of sales flats
        """
        self.connect()

        query = """
            SELECT sf.*,
                   ROW_NUMBER() OVER (PARTITION BY sf.flat_id ORDER BY sf.query_date DESC) as rn
            FROM sales_flats sf
            WHERE sf.residential_complex = ?
            AND DATE(sf.query_date) BETWEEN DATE(?) AND DATE(?)
        """

        cursor = self.conn.execute(query, (jk_name, start_date, end_date))
        sales = [dict(row) for row in cursor.fetchall() if row["rn"] == 1]

        self.disconnect()
        return sales

    def get_flat_info_by_id(self, flat_id: str) -> Optional[FlatInfo]:
        """
        Get flat information by flat_id, checking both rental and sales tables.
        Returns the most recent entry (by query_date).

        :param flat_id: str, flat ID to get info for
        :return: Optional[FlatInfo], flat information or None if not found
        """
        self.connect()

        # Try to find the flat in rental_flats first
        cursor = self.conn.execute(
            """
            SELECT flat_id, price, area, flat_type, residential_complex, floor,
                   total_floors, construction_year, parking, description, url, query_date,
                   archived, scraped_at, city
            FROM rental_flats
            WHERE flat_id = ?
            ORDER BY query_date DESC
            LIMIT 1
        """,
            (flat_id,),
        )

        row = cursor.fetchone()
        if row:
            flat_info = FlatInfo(
                flat_id=row[0],
                price=row[1],
                area=row[2],
                flat_type=row[3],
                residential_complex=row[4],
                floor=row[5],
                total_floors=row[6],
                construction_year=row[7],
                parking=row[8],
                description=row[9],
                is_rental=True,
                archived=bool(row[12] if len(row) > 12 else 0),
                scraped_at=row[13] if len(row) > 13 else None,
                city=row[14] if len(row) > 14 else None,
            )
            flat_info.url = (
                row[10] if row[10] else f"https://krisha.kz/a/show/{flat_id}"
            )
            self.disconnect()
            return flat_info

        # If not found in rental_flats, try sales_flats
        cursor = self.conn.execute(
            """
            SELECT flat_id, price, area, flat_type, residential_complex, floor,
                   total_floors, construction_year, parking, description, url, query_date,
                   archived, scraped_at, published_at, created_at, city
            FROM sales_flats
            WHERE flat_id = ?
            ORDER BY query_date DESC
            LIMIT 1
        """,
            (flat_id,),
        )

        row = cursor.fetchone()
        if row:
            flat_info = FlatInfo(
                flat_id=row[0],
                price=row[1],
                area=row[2],
                flat_type=row[3],
                residential_complex=row[4],
                floor=row[5],
                total_floors=row[6],
                construction_year=row[7],
                parking=row[8],
                description=row[9],
                is_rental=False,
                archived=bool(row[12] if len(row) > 12 else 0),
                scraped_at=row[13] if len(row) > 13 else None,
                published_at=row[14] if len(row) > 14 else None,
                created_at=row[15] if len(row) > 15 else None,
                city=row[16] if len(row) > 16 else None,
            )
            flat_info.url = (
                row[10] if row[10] else f"https://krisha.kz/a/show/{flat_id}"
            )
            self.disconnect()
            return flat_info

        self.disconnect()
        return None

    def get_similar_rentals_by_area_and_complex(
        self,
        residential_complex: Optional[str],
        area_min: float,
        area_max: float,
        city: Optional[str] = None,
    ) -> List[FlatInfo]:
        """
        Get similar rental flats by residential complex and area range.

        :param residential_complex: Optional[str], residential complex name (can be None or use LIKE pattern)
        :param area_min: float, minimum area
        :param area_max: float, maximum area
        :param city: Optional[str], city to filter by (e.g. "Алматы")
        :return: List[FlatInfo], list of similar rental flats
        """
        self.connect()

        jk_arg = f"%{residential_complex}%" if residential_complex else "%"
        params = [jk_arg, area_min, area_max]

        city_filter = ""
        if city:
            city_filter = "AND city = ?"
            params.append(city)

        query = f"""
            SELECT flat_id, price, area, flat_type, residential_complex, floor,
                   construction_year, total_floors, parking, description
            FROM rental_flats
            WHERE residential_complex LIKE ?
            AND area BETWEEN ? AND ?
            AND (archived = 0 OR archived IS NULL)
            {city_filter}
            AND query_date = (
                SELECT MAX(r2.query_date) FROM rental_flats r2
                WHERE r2.flat_id = rental_flats.flat_id
            )
            ORDER BY price ASC
        """

        cursor = self.conn.execute(query, params)

        similar_rentals = []
        for row in cursor.fetchall():
            rental_flat = FlatInfo(
                flat_id=row[0],
                price=row[1],
                area=row[2],
                flat_type=row[3],
                residential_complex=row[4],
                floor=row[5],
                construction_year=row[6],
                total_floors=row[7] if row[7] else 0,
                parking=row[8] if row[8] else False,
                description=row[9] if row[9] else "",
                is_rental=True,
            )
            similar_rentals.append(rental_flat)

        self.disconnect()
        return similar_rentals

    def get_similar_sales_by_area_and_complex(
        self,
        residential_complex: Optional[str],
        area_min: float,
        area_max: float,
        city: Optional[str] = None,
    ) -> List[FlatInfo]:
        """
        Get similar sales flats by residential complex and area range.

        :param residential_complex: Optional[str], residential complex name (can be None or use LIKE pattern)
        :param area_min: float, minimum area
        :param area_max: float, maximum area
        :param city: Optional[str], city to filter by (e.g. "Алматы")
        :return: List[FlatInfo], list of similar sales flats
        """
        self.connect()

        jk_arg = f"%{residential_complex}%" if residential_complex else "%"
        params = [jk_arg, area_min, area_max]

        city_filter = ""
        if city:
            city_filter = "AND city = ?"
            params.append(city)

        query = f"""
            SELECT flat_id, price, area, flat_type, residential_complex, floor,
                   construction_year, total_floors, parking, description
            FROM sales_flats
            WHERE residential_complex LIKE ?
            AND area BETWEEN ? AND ?
            AND (archived = 0 OR archived IS NULL)
            {city_filter}
            AND query_date = (
                SELECT MAX(s2.query_date) FROM sales_flats s2
                WHERE s2.flat_id = sales_flats.flat_id
            )
            ORDER BY price ASC
        """

        cursor = self.conn.execute(query, params)

        similar_sales = []
        for row in cursor.fetchall():
            sales_flat = FlatInfo(
                flat_id=row[0],
                price=row[1],
                area=row[2],
                flat_type=row[3],
                residential_complex=row[4],
                floor=row[5],
                construction_year=row[6],
                total_floors=row[7] if row[7] else 0,
                parking=row[8] if row[8] else False,
                description=row[9] if row[9] else "",
                is_rental=False,
            )
            similar_sales.append(sales_flat)

        self.disconnect()
        return similar_sales

    def _calculate_median(self, values: List[float]) -> float:
        """
        Calculate median of a list of values.

        :param values: List[float], list of values
        :return: float, median value
        """
        if not values:
            return None

        sorted_values = sorted(values)
        n = len(sorted_values)

        if n % 2 == 0:
            return (sorted_values[n // 2 - 1] + sorted_values[n // 2]) / 2
        else:
            return sorted_values[n // 2]

    # Opportunity Analysis operations
    def insert_opportunity_analysis_batch(
        self, opportunities: List[Dict], run_timestamp: str
    ) -> int:
        """
        Insert a batch of opportunity analysis results with the same run timestamp.

        :param opportunities: List[Dict], list of opportunity dictionaries
        :param run_timestamp: str, timestamp for this analysis run (format: YYYY-MM-DD HH:MM:SS)
        :return: int, number of opportunities successfully inserted
        """
        self.connect()

        inserted_count = 0
        try:
            for opp in opportunities:
                self.conn.execute(
                    """
                    INSERT INTO opportunity_analysis (
                        run_timestamp, rank, flat_id, residential_complex, price, area,
                        flat_type, floor, total_floors, construction_year, parking,
                        discount_percentage_vs_median, median_price, mean_price,
                        min_price, max_price, sample_size, query_date, url, description
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                    (
                        run_timestamp,
                        opp.get("rank"),
                        opp.get("flat_id"),
                        opp.get("residential_complex"),
                        opp.get("price"),
                        opp.get("area"),
                        opp.get("flat_type"),
                        opp.get("floor") if opp.get("floor") else None,
                        opp.get("total_floors") if opp.get("total_floors") else None,
                        opp.get("construction_year")
                        if opp.get("construction_year")
                        else None,
                        opp.get("parking") if opp.get("parking") else None,
                        opp.get("discount_percentage_vs_median"),
                        opp.get("median_price"),
                        opp.get("mean_price"),
                        opp.get("min_price"),
                        opp.get("max_price"),
                        opp.get("sample_size"),
                        opp.get("query_date"),
                        opp.get("url"),
                        opp.get("description") if opp.get("description") else None,
                    ),
                )
                inserted_count += 1

            self.conn.commit()
            logging.info(
                f"Inserted {inserted_count} opportunities with run_timestamp: {run_timestamp}"
            )
        except Exception as e:
            logging.error(f"Error inserting opportunity analysis batch: {e}")
            self.conn.rollback()
            raise
        finally:
            self.disconnect()

        return inserted_count

    def insert_pipeline_run(self, stats: Dict) -> bool:
        """
        Insert a pipeline run stats row.

        :param stats: dict with keys: started_at, finished_at, duration_seconds,
                      city, jks_total, jks_successful, jks_failed, flats_scraped,
                      error_breakdown (dict of error_type -> count)
        :return: bool, True if successful
        """
        self.connect()
        try:
            # Compute legacy aggregate columns from breakdown
            error_breakdown = stats.get("error_breakdown", {})
            rate_limited = error_breakdown.get("http_429", 0)
            http_errors = sum(
                v for k, v in error_breakdown.items() if k.startswith("http_")
            )
            request_errors = sum(
                v
                for k, v in error_breakdown.items()
                if k in ("timeout", "connection_error", "other_error")
            )

            import json

            self.conn.execute(
                """
                INSERT INTO pipeline_runs (
                    started_at, finished_at, duration_seconds, city,
                    jks_total, jks_successful, jks_failed, flats_scraped,
                    rate_limited, http_errors, request_errors, error_breakdown
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    stats["started_at"],
                    stats["finished_at"],
                    stats["duration_seconds"],
                    stats.get("city"),
                    stats["jks_total"],
                    stats["jks_successful"],
                    stats["jks_failed"],
                    stats["flats_scraped"],
                    rate_limited,
                    http_errors,
                    request_errors,
                    json.dumps(error_breakdown) if error_breakdown else None,
                ),
            )
            self.conn.commit()
            return True
        except Exception as e:
            logging.error(f"Error inserting pipeline run: {e}")
            return False
        finally:
            self.disconnect()

    def get_latest_pipeline_run(self) -> Optional[Dict]:
        """
        Get the most recent pipeline run stats.

        :return: Optional[Dict], latest run stats or None
        """
        self.connect()
        try:
            self.conn.row_factory = sqlite3.Row
            cursor = self.conn.execute(
                """
                SELECT started_at, finished_at, duration_seconds, city,
                       jks_total, jks_successful, jks_failed, flats_scraped,
                       rate_limited, http_errors, request_errors, error_breakdown
                FROM pipeline_runs
                ORDER BY finished_at DESC
                LIMIT 1
                """
            )
            row = cursor.fetchone()
            return dict(row) if row else None
        except Exception as e:
            logging.error(f"Error fetching latest pipeline run: {e}")
            return None
        finally:
            self.disconnect()

    def get_pipeline_runs_history(self, limit: int = 90) -> Dict:
        """
        Get pipeline run history for the tech status dashboard.

        :param limit: int, max number of runs to return (most recent first)
        :return: dict with keys 'runs' (list of dicts) and 'kpis' (aggregated stats)
        """
        self.connect()
        try:
            self.conn.row_factory = sqlite3.Row
            cursor = self.conn.execute(
                """
                SELECT started_at, finished_at, duration_seconds, city,
                       jks_total, jks_successful, jks_failed, flats_scraped,
                       rate_limited, http_errors, request_errors, error_breakdown
                FROM pipeline_runs
                ORDER BY finished_at DESC
                LIMIT ?
                """,
                (limit,),
            )
            rows = [dict(row) for row in cursor.fetchall()]

            if not rows:
                return {
                    "runs": [],
                    "kpis": {
                        "total_runs": 0,
                        "avg_duration": 0,
                        "total_flats_scraped": 0,
                        "total_errors": 0,
                        "last_run": None,
                    },
                }

            total_runs = len(rows)
            total_errors = sum(
                r["rate_limited"] + r["http_errors"] + r["request_errors"] for r in rows
            )
            avg_duration = sum(r["duration_seconds"] for r in rows) / total_runs
            total_flats = sum(r["flats_scraped"] for r in rows)

            return {
                "runs": rows,
                "kpis": {
                    "total_runs": total_runs,
                    "avg_duration": round(avg_duration, 1),
                    "total_flats_scraped": total_flats,
                    "total_errors": total_errors,
                    "last_run": rows[0],
                },
            }
        except Exception as e:
            logging.error(f"Error fetching pipeline runs history: {e}")
            return {
                "runs": [],
                "kpis": {
                    "total_runs": 0,
                    "avg_duration": 0,
                    "total_flats_scraped": 0,
                    "total_errors": 0,
                    "last_run": None,
                },
            }
        finally:
            self.disconnect()

    def get_latest_opportunity_analysis_run_timestamp(self) -> Optional[str]:
        """
        Get the latest run timestamp from opportunity analysis.

        :return: Optional[str], latest run timestamp or None if no runs exist
        """
        self.connect()

        cursor = self.conn.execute(
            """
            SELECT DISTINCT run_timestamp 
            FROM opportunity_analysis 
            ORDER BY run_timestamp DESC 
            LIMIT 1
        """
        )

        row = cursor.fetchone()
        result = row[0] if row else None
        self.disconnect()
        return result

    def get_opportunity_analysis_by_timestamp(self, run_timestamp: str) -> List[Dict]:
        """
        Get all opportunity analysis results for a specific run timestamp.

        :param run_timestamp: str, run timestamp to query
        :return: List[Dict], list of opportunity analysis results
        """
        self.connect()

        cursor = self.conn.execute(
            """
            SELECT * FROM opportunity_analysis 
            WHERE run_timestamp = ?
            ORDER BY rank ASC
        """,
            (run_timestamp,),
        )

        result = [dict(row) for row in cursor.fetchall()]
        self.disconnect()
        return result

    def get_all_opportunity_analysis_run_timestamps(self) -> List[str]:
        """
        Get all distinct run timestamps from opportunity analysis, ordered by most recent first.

        :return: List[str], list of run timestamps
        """
        self.connect()

        cursor = self.conn.execute(
            """
            SELECT DISTINCT run_timestamp 
            FROM opportunity_analysis 
            ORDER BY run_timestamp DESC
        """
        )

        result = [row[0] for row in cursor.fetchall()]
        self.disconnect()
        return result

    def get_top_opportunities(
        self,
        limit: int = 5,
        max_price: int = None,
        max_age_days: int = None,
        city: str = None,
        flat_types: List[str] = None,
    ) -> List[Dict]:
        """
        Get top N opportunities from the latest analysis run.

        :param limit: int, number of opportunities to return (default: 5)
        :param max_price: int, maximum price filter (optional)
        :param max_age_days: int, only include opportunities from runs within this many days (optional)
        :param city: str, city name in Cyrillic (e.g. "Алматы") to filter by (optional)
        :param flat_types: List[str], flat types to include (e.g. ["1BR", "2BR"]) (optional, None = all)
        :return: List[Dict], list of top opportunities
        """
        self.connect()

        # Build query with optional filters
        conditions = []
        params = []

        if max_age_days is not None:
            conditions.append("oa.run_timestamp >= datetime('now', ?)")
            params.append(f"-{max_age_days} days")

        if max_price is not None:
            conditions.append("oa.price <= ?")
            params.append(max_price)

        # Exclude ignored opportunities and blacklisted JKs
        conditions.append(
            "oa.flat_id NOT IN (SELECT flat_id FROM ignored_opportunities)"
        )
        conditions.append(
            "oa.residential_complex NOT IN (SELECT name FROM blacklisted_jks)"
        )

        # Exclude JKs in blacklisted districts
        conditions.append(
            """NOT EXISTS (
                SELECT 1 FROM residential_complexes rc2
                JOIN blacklisted_districts bd ON bd.city = rc2.city AND bd.district = rc2.district
                WHERE rc2.name = oa.residential_complex
            )"""
        )

        # City filter: use per-flat city from sales_flats (handles JK names shared across cities)
        if city is not None:
            conditions.append(
                """EXISTS (SELECT 1 FROM sales_flats sf
                    WHERE sf.flat_id = oa.flat_id AND sf.city = ?)"""
            )
            params.append(city)

        if flat_types is not None:
            placeholders = ",".join("?" for _ in flat_types)
            conditions.append(f"oa.flat_type IN ({placeholders})")
            params.extend(flat_types)

        where_clause = ""
        if conditions:
            where_clause = "AND " + " AND ".join(conditions)

        query = f"""
            SELECT oa.* FROM opportunity_analysis oa
            WHERE oa.run_timestamp = (
                SELECT MAX(run_timestamp) FROM opportunity_analysis
            )
            {where_clause}
            ORDER BY oa.discount_percentage_vs_median DESC
            LIMIT ?
        """
        params.append(limit)

        cursor = self.conn.execute(query, params)

        result = [dict(row) for row in cursor.fetchall()]
        # Re-rank after filtering so numbers are always 1..N
        for i, row in enumerate(result, 1):
            row["rank"] = i
        self.disconnect()
        return result

    def ignore_opportunity(self, flat_id: str) -> bool:
        """
        Add a flat to the ignored opportunities list.

        :param flat_id: str, flat ID to ignore
        :return: bool, True if successful
        """
        self.connect()
        try:
            self.conn.execute(
                "INSERT OR IGNORE INTO ignored_opportunities (flat_id) VALUES (?)",
                (flat_id,),
            )
            self.conn.commit()
            return True
        except Exception as e:
            logging.error(f"Error ignoring opportunity {flat_id}: {e}")
            return False
        finally:
            self.disconnect()

    def unignore_opportunity(self, flat_id: str) -> bool:
        """
        Remove a flat from the ignored opportunities list.

        :param flat_id: str, flat ID to unignore
        :return: bool, True if successful
        """
        self.connect()
        try:
            self.conn.execute(
                "DELETE FROM ignored_opportunities WHERE flat_id = ?",
                (flat_id,),
            )
            self.conn.commit()
            return True
        except Exception as e:
            logging.error(f"Error unignoring opportunity {flat_id}: {e}")
            return False
        finally:
            self.disconnect()

    def get_price_movers(self, city: str = None, limit: int = 5) -> Dict:
        """
        Get JKs with biggest price increases and decreases between earliest and latest query dates.

        :param city: str, city name in Cyrillic to filter by (optional)
        :param limit: int, number of top movers to return per direction
        :return: Dict with 'risers', 'fallers', 'old_date', 'new_date' keys
        """
        self.connect()

        city_join = ""
        city_condition = ""
        city_params = ()
        if city is not None:
            city_join = (
                "JOIN residential_complexes rc ON s.residential_complex = rc.name"
            )
            city_condition = "AND rc.city = ?"
            city_params = (city,)

        # Get the two most recent distinct query dates
        cursor = self.conn.execute(
            "SELECT DISTINCT query_date FROM sales_flats ORDER BY query_date DESC LIMIT 2"
        )
        dates = [row[0] for row in cursor.fetchall()]
        if len(dates) < 2:
            self.disconnect()
            return {"risers": [], "fallers": [], "old_date": None, "new_date": None}

        new_date, old_date = dates[0], dates[1]

        query_template = (
            "WITH old_prices AS ("
            "  SELECT s.residential_complex, AVG(s.price / s.area) as avg_price_m2, COUNT(*) as cnt"
            f"  FROM sales_flats s {city_join}"
            f"  WHERE s.query_date = ? AND s.area > 0 AND s.price / s.area < 5000000 {city_condition}"
            "  GROUP BY s.residential_complex HAVING cnt >= 3"
            "), new_prices AS ("
            "  SELECT s.residential_complex, AVG(s.price / s.area) as avg_price_m2, COUNT(*) as cnt"
            f"  FROM sales_flats s {city_join}"
            f"  WHERE s.query_date = ? AND s.area > 0 AND s.price / s.area < 5000000 {city_condition}"
            "  GROUP BY s.residential_complex HAVING cnt >= 3"
            ") SELECT o.residential_complex, o.avg_price_m2 as old_price, n.avg_price_m2 as new_price,"
            "  ((n.avg_price_m2 - o.avg_price_m2) / o.avg_price_m2 * 100) as pct_change,"
            "  o.cnt as count_old, n.cnt as count_new"
            " FROM old_prices o JOIN new_prices n ON o.residential_complex = n.residential_complex"
            " ORDER BY pct_change {order}"
            " LIMIT ?"
        )

        # Get risers (params: old_date, [city], new_date, [city], limit)
        risers_query = query_template.format(order="DESC")
        cursor = self.conn.execute(
            risers_query,
            (old_date,) + city_params + (new_date,) + city_params + (limit,),
        )
        risers = [dict(row) for row in cursor.fetchall()]

        # Get fallers
        fallers_query = query_template.format(order="ASC")
        cursor = self.conn.execute(
            fallers_query,
            (old_date,) + city_params + (new_date,) + city_params + (limit,),
        )
        fallers = [dict(row) for row in cursor.fetchall()]

        self.disconnect()
        return {
            "risers": risers,
            "fallers": fallers,
            "old_date": old_date,
            "new_date": new_date,
        }

    def get_best_rental_yields(self, city: str = None, limit: int = 10) -> List[Dict]:
        """
        Get JKs with best rental yields (annual rent / sale price).

        :param city: str, city name in Cyrillic to filter by (optional)
        :param limit: int, number of results to return
        :return: List[Dict] with jk_name, avg_rent, avg_sale_price, yield_pct, rental_count, sales_count
        """
        self.connect()

        city_join = ""
        city_condition = ""
        city_params = ()
        if city is not None:
            city_join = (
                "JOIN residential_complexes rc ON r.residential_complex = rc.name"
            )
            city_condition = "AND rc.city = ?"
            city_params = (city,)

        query = f"""
            SELECT r.residential_complex as jk_name,
                   AVG(r.price) as avg_rent,
                   s.avg_sale_price,
                   (AVG(r.price) * 12.0 / s.avg_sale_price * 100) as yield_pct,
                   COUNT(DISTINCT r.flat_id) as rental_count,
                   s.sales_count
            FROM rental_flats r
            {city_join}
            JOIN (
                SELECT residential_complex, AVG(price) as avg_sale_price, COUNT(DISTINCT flat_id) as sales_count
                FROM sales_flats
                WHERE query_date = (SELECT MAX(query_date) FROM sales_flats)
                GROUP BY residential_complex
                HAVING sales_count >= 3
            ) s ON r.residential_complex = s.residential_complex
            WHERE 1=1 {city_condition}
            GROUP BY r.residential_complex
            HAVING rental_count >= 3
            ORDER BY yield_pct DESC
            LIMIT ?
        """

        cursor = self.conn.execute(query, city_params + (limit,))
        result = [dict(row) for row in cursor.fetchall()]
        self.disconnect()
        return result

    def get_market_velocity(self, city: str = None) -> Dict:
        """
        Get market turnover stats between the two most recent query dates.

        :param city: str, city name in Cyrillic to filter by (optional)
        :return: Dict with removed, new_listings, stable, total_old, total_new, turnover_pct, old_date, new_date
        """
        self.connect()

        # Get the two most recent distinct query dates
        cursor = self.conn.execute(
            "SELECT DISTINCT query_date FROM sales_flats ORDER BY query_date DESC LIMIT 2"
        )
        dates = [row[0] for row in cursor.fetchall()]
        if len(dates) < 2:
            self.disconnect()
            return {
                "removed": 0,
                "new_listings": 0,
                "stable": 0,
                "total_old": 0,
                "total_new": 0,
                "turnover_pct": 0,
                "old_date": None,
                "new_date": None,
            }

        new_date, old_date = dates[0], dates[1]

        city_join = ""
        city_condition = ""
        city_params = ()
        if city is not None:
            city_join = (
                "JOIN residential_complexes rc ON s.residential_complex = rc.name"
            )
            city_condition = "AND rc.city = ?"
            city_params = (city,)

        # Flats in old date
        cursor = self.conn.execute(
            f"SELECT COUNT(DISTINCT s.flat_id) FROM sales_flats s {city_join} WHERE s.query_date = ? {city_condition}",
            (old_date,) + city_params,
        )
        total_old = cursor.fetchone()[0]

        # Flats in new date
        cursor = self.conn.execute(
            f"SELECT COUNT(DISTINCT s.flat_id) FROM sales_flats s {city_join} WHERE s.query_date = ? {city_condition}",
            (new_date,) + city_params,
        )
        total_new = cursor.fetchone()[0]

        # Removed (in old but not new)
        cursor = self.conn.execute(
            f"""SELECT COUNT(*) FROM (
                SELECT DISTINCT s.flat_id FROM sales_flats s {city_join} WHERE s.query_date = ? {city_condition}
                EXCEPT
                SELECT DISTINCT s.flat_id FROM sales_flats s {city_join} WHERE s.query_date = ? {city_condition}
            )""",
            (old_date,) + city_params + (new_date,) + city_params,
        )
        removed = cursor.fetchone()[0]

        # New listings (in new but not old)
        cursor = self.conn.execute(
            f"""SELECT COUNT(*) FROM (
                SELECT DISTINCT s.flat_id FROM sales_flats s {city_join} WHERE s.query_date = ? {city_condition}
                EXCEPT
                SELECT DISTINCT s.flat_id FROM sales_flats s {city_join} WHERE s.query_date = ? {city_condition}
            )""",
            (new_date,) + city_params + (old_date,) + city_params,
        )
        new_listings = cursor.fetchone()[0]

        stable = total_old - removed
        turnover_pct = (removed / total_old * 100) if total_old > 0 else 0

        self.disconnect()
        return {
            "removed": removed,
            "new_listings": new_listings,
            "stable": stable,
            "total_old": total_old,
            "total_new": total_new,
            "turnover_pct": round(turnover_pct, 1),
            "old_date": old_date,
            "new_date": new_date,
        }

    def get_flat_first_seen(self, flat_id: str) -> Optional[str]:
        """
        Get the earliest query_date for a flat (first time we saw it).

        :param flat_id: str, flat ID to look up
        :return: Optional[str], earliest query_date string or None
        """
        self.connect()

        cursor = self.conn.execute(
            "SELECT MIN(query_date) FROM sales_flats WHERE flat_id = ?", (flat_id,)
        )
        row = cursor.fetchone()
        if row and row[0]:
            self.disconnect()
            return row[0]

        cursor = self.conn.execute(
            "SELECT MIN(query_date) FROM rental_flats WHERE flat_id = ?", (flat_id,)
        )
        row = cursor.fetchone()
        self.disconnect()
        return row[0] if row and row[0] else None

    def get_jk_liquidity_score(self, residential_complex: str) -> Optional[Dict]:
        """Deprecated: use get_jk_turnover() instead."""
        return self.get_jk_turnover(residential_complex, days=30)

    def get_jk_turnover(
        self, residential_complex: str, days: int = 30
    ) -> Optional[Dict]:
        """
        Turnover for a JK: % of flats that disappeared (sold) over a given window.

        Compares the scrape date closest to `days` ago against the latest scrape date.
        Flats present in the old date but absent in the latest are counted as sold.

        :param residential_complex: str, JK name
        :param days: int, lookback window in days
        :return: Optional[Dict] with removed, total, turnover_pct, old_date, new_date
        """
        self.connect()

        # Find the latest scrape date for this JK
        cursor = self.conn.execute(
            """SELECT MAX(query_date) FROM sales_flats
               WHERE residential_complex = ?""",
            (residential_complex,),
        )
        row = cursor.fetchone()
        if not row or not row[0]:
            self.disconnect()
            return None
        new_date = row[0]

        # Find the scrape date closest to `days` ago (but at least 1 day before new_date)
        cursor = self.conn.execute(
            """SELECT DISTINCT query_date FROM sales_flats
               WHERE residential_complex = ? AND query_date < ?
               ORDER BY ABS(JULIANDAY(?) - JULIANDAY(query_date) - ?) ASC
               LIMIT 1""",
            (residential_complex, new_date, new_date, days),
        )
        row = cursor.fetchone()
        if not row:
            self.disconnect()
            return None
        old_date = row[0]

        # Flats in old date
        cursor = self.conn.execute(
            """SELECT COUNT(DISTINCT flat_id) FROM sales_flats
               WHERE residential_complex = ? AND query_date = ?""",
            (residential_complex, old_date),
        )
        total_old = cursor.fetchone()[0]

        if total_old == 0:
            self.disconnect()
            return None

        # Removed (in old but not in latest)
        cursor = self.conn.execute(
            """SELECT COUNT(*) FROM (
                SELECT DISTINCT flat_id FROM sales_flats
                WHERE residential_complex = ? AND query_date = ?
                EXCEPT
                SELECT DISTINCT flat_id FROM sales_flats
                WHERE residential_complex = ? AND query_date = ?
            )""",
            (residential_complex, old_date, residential_complex, new_date),
        )
        removed = cursor.fetchone()[0]

        turnover_pct = removed / total_old * 100

        self.disconnect()
        return {
            "removed": removed,
            "total": total_old,
            "turnover_pct": round(turnover_pct, 1),
            "old_date": old_date,
            "new_date": new_date,
        }

    def get_price_per_sqm_rankings(
        self, city: str = None, limit: int = 15
    ) -> List[Dict]:
        """
        Get JKs ranked by average price per square meter.

        :param city: str, city name in Cyrillic to filter by (optional)
        :param limit: int, number of results to return
        :return: List[Dict] with jk_name, avg_price_sqm, min_price_sqm, max_price_sqm, count
        """
        self.connect()

        city_join = ""
        city_condition = ""
        city_params = ()
        if city is not None:
            city_join = (
                "JOIN residential_complexes rc ON s.residential_complex = rc.name"
            )
            city_condition = "AND rc.city = ?"
            city_params = (city,)

        query = f"""
            SELECT s.residential_complex as jk_name,
                   COUNT(*) as count,
                   AVG(s.price / s.area) as avg_price_sqm,
                   MIN(s.price / s.area) as min_price_sqm,
                   MAX(s.price / s.area) as max_price_sqm
            FROM sales_flats s
            {city_join}
            WHERE s.query_date = (SELECT MAX(query_date) FROM sales_flats)
              AND s.area > 0
              {city_condition}
            GROUP BY s.residential_complex
            HAVING count >= 5
            ORDER BY avg_price_sqm DESC
            LIMIT ?
        """

        cursor = self.conn.execute(query, city_params + (limit,))
        result = [dict(row) for row in cursor.fetchall()]
        self.disconnect()
        return result

    def is_flat_archived(self, flat_id: str, is_rental: bool = False) -> bool:
        """
        Check if a flat is archived.

        :param flat_id: str, flat ID to check
        :param is_rental: bool, True if checking rental_flats, False for sales_flats
        :return: bool, True if flat is archived
        """
        self.connect()

        table = "rental_flats" if is_rental else "sales_flats"
        cursor = self.conn.execute(
            f"""
            SELECT archived 
            FROM {table} 
            WHERE flat_id = ?
            ORDER BY query_date DESC
            LIMIT 1
        """,
            (flat_id,),
        )

        row = cursor.fetchone()
        if row:
            archived_value = row[0]
            result = archived_value == 1 if archived_value is not None else False
        else:
            result = False

        self.disconnect()
        return result

    def get_non_archived_flat_ids_for_jk(
        self, jk_name: str, is_rental: bool = False
    ) -> List[str]:
        """
        Get all non-archived flat IDs for a specific residential complex.

        :param jk_name: str, name of the residential complex
        :param is_rental: bool, True for rental_flats, False for sales_flats
        :return: List[str], list of flat IDs
        """
        self.connect()

        table = "rental_flats" if is_rental else "sales_flats"
        query = f"""
            SELECT DISTINCT flat_id
            FROM {table}
            WHERE residential_complex = ? AND (archived = 0 OR archived IS NULL)
        """

        cursor = self.conn.execute(query, (jk_name,))
        flat_ids = [row[0] for row in cursor.fetchall()]

        self.disconnect()
        return flat_ids

    def mark_flat_as_archived(self, flat_id: str, is_rental: bool = False) -> bool:
        """
        Mark a flat as archived in the database.

        :param flat_id: str, flat ID to mark as archived
        :param is_rental: bool, True for rental_flats, False for sales_flats
        :return: bool, True if successful
        """
        self.connect()

        table = "rental_flats" if is_rental else "sales_flats"
        query = f"""
            UPDATE {table}
            SET archived = 1, updated_at = CURRENT_TIMESTAMP
            WHERE flat_id = ?
        """

        cursor = self.conn.execute(query, (flat_id,))
        self.conn.commit()
        rows_affected = cursor.rowcount

        self.disconnect()
        return rows_affected > 0

    def get_historical_sales_for_jk(self, jk_name: str, start_date: str) -> List[dict]:
        """
        Get historical sales data for a residential complex from a start date.

        :param jk_name: str, name of the residential complex
        :param start_date: str, start date in format 'YYYY-MM-DD'
        :return: List[dict], historical sales data with query_date, flat_type, price, area, floor, total_floors, flat_id, url
        """
        self.connect()

        query = """
            SELECT query_date, flat_type, price, area, floor, total_floors, flat_id, url
            FROM sales_flats
            WHERE residential_complex = ? AND query_date >= ?
            ORDER BY query_date, flat_type
        """

        cursor = self.conn.execute(query, (jk_name, start_date))
        historical_data = [dict(row) for row in cursor.fetchall()]

        self.disconnect()
        return historical_data

    def get_all_jk_names_from_sales(self) -> List[str]:
        """
        Get list of all distinct residential complex names from sales_flats table.

        :return: List[str], list of JK names sorted alphabetically
        """
        self.connect()

        query = """
            SELECT DISTINCT residential_complex
            FROM sales_flats
            WHERE residential_complex IS NOT NULL
            ORDER BY residential_complex
        """

        cursor = self.conn.execute(query)
        result = [row[0] for row in cursor.fetchall()]

        self.disconnect()
        return result

    def get_jk_sales_summary(self, jk_name: str) -> dict:
        """
        Get a quick summary of sales data for a residential complex.

        :param jk_name: str, name of the residential complex
        :return: dict, summary statistics with jk_name, total_sales, date_range, flat_type_distribution
        """
        self.connect()

        # Get total sales count
        count_query = "SELECT COUNT(*) FROM sales_flats WHERE residential_complex = ?"
        total_sales = self.conn.execute(count_query, (jk_name,)).fetchone()[0]

        # Get date range
        date_query = """
            SELECT MIN(query_date) as earliest, MAX(query_date) as latest
            FROM sales_flats
            WHERE residential_complex = ?
        """
        date_result = self.conn.execute(date_query, (jk_name,)).fetchone()

        # Get flat type distribution
        type_query = """
            SELECT flat_type, COUNT(*) as count
            FROM sales_flats
            WHERE residential_complex = ?
            GROUP BY flat_type
            ORDER BY count DESC
        """
        type_distribution = dict(self.conn.execute(type_query, (jk_name,)).fetchall())

        result = {
            "jk_name": jk_name,
            "total_sales": total_sales,
            "date_range": {"earliest": date_result[0], "latest": date_result[1]},
            "flat_type_distribution": type_distribution,
        }

        self.disconnect()
        return result

    def get_all_jks_excluding_blacklisted(self, city: str = "almaty") -> List[Dict]:
        """
        Get all residential complexes (JKs) from the residential_complexes table, excluding blacklisted ones, filtered by city.

        :param city: str, city name to filter by (default: "almaty")
        :return: List[Dict], list of JK information with keys: residential_complex, complex_id, city, district
        """
        self.connect()

        try:
            # Support both Cyrillic and Latin city names
            city_variants = [city.lower(), city.capitalize(), city.title()]
            # Also check for Cyrillic Алматы if city is almaty
            if city.lower() == "almaty":
                city_variants.extend(["Алматы", "алматы"])

            placeholders = ",".join(["?"] * len(city_variants))
            query = f"""
                SELECT name as residential_complex, complex_id, city, district
                FROM residential_complexes
                WHERE name NOT IN (
                    SELECT name FROM blacklisted_jks
                )
                AND (city IN ({placeholders}))
                AND NOT EXISTS (
                    SELECT 1 FROM blacklisted_districts bd
                    WHERE bd.city = residential_complexes.city
                    AND bd.district = residential_complexes.district
                )
                ORDER BY name
            """
            cursor = self.conn.execute(query, city_variants)

            jks = [dict(row) for row in cursor.fetchall()]
            return jks

        except Exception as e:
            logging.error(f"Error fetching JKs from database: {e}")
            return []
        finally:
            self.disconnect()

    def get_all_jks_excluding_blacklisted_no_city_filter(self) -> List[Dict]:
        """
        Get all residential complexes (JKs) from the residential_complexes table, excluding blacklisted ones, without city filtering.

        :return: List[Dict], list of JK information with keys: residential_complex, complex_id, city, district
        """
        self.connect()

        try:
            cursor = self.conn.execute("""
                SELECT name as residential_complex, complex_id, city, district
                FROM residential_complexes
                WHERE name NOT IN (
                    SELECT name FROM blacklisted_jks
                )
                AND NOT EXISTS (
                    SELECT 1 FROM blacklisted_districts bd
                    WHERE bd.city = residential_complexes.city
                    AND bd.district = residential_complexes.district
                )
                ORDER BY name
            """)
            jks = [dict(row) for row in cursor.fetchall()]
            return jks
        except Exception as e:
            logging.error(f"Error fetching JKs from database: {e}")
            return []
        finally:
            self.disconnect()

    def find_matching_jk_names(self, search_term: str) -> List[str]:
        """
        Find all JK names that match a search term (case-insensitive partial match).
        Searches both residential_complexes table and existing flats tables.

        :param search_term: str, search term to match against JK names
        :return: List[str], list of matching JK names
        """
        self.connect()

        try:
            matching_names = set()
            search_pattern = f"%{search_term}%"

            # Search in residential_complexes table
            cursor = self.conn.execute(
                """
                SELECT DISTINCT name FROM residential_complexes
                WHERE LOWER(name) LIKE LOWER(?)
                """,
                (search_pattern,),
            )
            for row in cursor.fetchall():
                matching_names.add(row[0])

            # Also search in sales_flats (in case JK exists there but not in residential_complexes)
            cursor = self.conn.execute(
                """
                SELECT DISTINCT residential_complex FROM sales_flats
                WHERE LOWER(residential_complex) LIKE LOWER(?)
                """,
                (search_pattern,),
            )
            for row in cursor.fetchall():
                if row[0]:
                    matching_names.add(row[0])

            # Also search in rental_flats
            cursor = self.conn.execute(
                """
                SELECT DISTINCT residential_complex FROM rental_flats
                WHERE LOWER(residential_complex) LIKE LOWER(?)
                """,
                (search_pattern,),
            )
            for row in cursor.fetchall():
                if row[0]:
                    matching_names.add(row[0])

            return sorted(list(matching_names))
        except Exception as e:
            logging.error(f"Error finding matching JK names: {e}")
            return []
        finally:
            self.disconnect()

    def get_residential_complexes_count(self) -> int:
        """
        Get total count of residential complexes in the database.

        :return: int, total count of residential complexes
        """
        self.connect()

        try:
            cursor = self.conn.execute("SELECT COUNT(*) FROM residential_complexes")
            result = cursor.fetchone()
            count = result[0] if result else 0
            return count
        except Exception as e:
            logging.error(f"Error getting residential complexes count: {e}")
            return 0
        finally:
            self.disconnect()

    def get_sample_residential_complexes(self, limit: int = 10) -> List[Dict]:
        """
        Get a sample of residential complexes from the database.

        :param limit: int, maximum number of complexes to return (default: 10)
        :return: List[Dict], list of JK information with keys: name, complex_id, city, district
        """
        self.connect()

        try:
            cursor = self.conn.execute(
                """
                SELECT name, complex_id, city, district 
                FROM residential_complexes 
                ORDER BY name 
                LIMIT ?
            """,
                (limit,),
            )
            complexes = [dict(row) for row in cursor.fetchall()]
            return complexes
        except Exception as e:
            logging.error(f"Error getting sample residential complexes: {e}")
            return []
        finally:
            self.disconnect()

    def get_residential_complex_by_complex_id(self, complex_id: str) -> Optional[Dict]:
        """
        Get residential complex by complex_id.

        :param complex_id: str, complex ID
        :return: Optional[Dict], complex information with keys: complex_id, city (or None if not found)
        """
        self.connect()

        try:
            cursor = self.conn.execute(
                "SELECT complex_id, city FROM residential_complexes WHERE complex_id = ?",
                (complex_id,),
            )
            row = cursor.fetchone()
            result = dict(row) if row else None
            return result
        except Exception as e:
            logging.error(f"Error getting residential complex by complex_id: {e}")
            return None
        finally:
            self.disconnect()

    def update_residential_complex_city_and_district(
        self, complex_id: str, city: str, district: str = None
    ) -> bool:
        """
        Update city and district for an existing residential complex.

        :param complex_id: str, complex ID
        :param city: str, city name
        :param district: str, district name (optional)
        :return: bool, True if successful
        """
        self.connect()

        try:
            self.conn.execute(
                """
                UPDATE residential_complexes 
                SET city = ?, district = ?
                WHERE complex_id = ?
            """,
                (city, district, complex_id),
            )
            self.conn.commit()
            return True
        except Exception as e:
            logging.error(f"Error updating residential complex city: {e}")
            self.conn.rollback()
            return False
        finally:
            self.disconnect()

    def get_residential_complex_by_name(self, name: str) -> Optional[Dict]:
        """
        Get a residential complex by name.

        :param name: str, JK name
        :return: Optional[Dict] with complex_id, name, city, district
        """
        self.connect()
        cursor = self.conn.execute(
            "SELECT complex_id, name, city, district FROM residential_complexes WHERE name = ? LIMIT 1",
            (name,),
        )
        row = cursor.fetchone()
        self.disconnect()
        if row:
            return {
                "complex_id": row[0],
                "name": row[1],
                "city": row[2],
                "district": row[3],
            }
        return None

    def update_residential_complex_district(
        self, complex_id: str, district: str
    ) -> bool:
        """
        Update only the district for an existing residential complex.
        Does NOT change the city.

        :param complex_id: str, complex ID
        :param district: str, district name
        :return: bool, True if successful
        """
        self.connect()

        try:
            self.conn.execute(
                "UPDATE residential_complexes SET district = ? WHERE complex_id = ?",
                (district, complex_id),
            )
            self.conn.commit()
            return True
        except Exception as e:
            logging.error(f"Error updating residential complex district: {e}")
            self.conn.rollback()
            return False
        finally:
            self.disconnect()

    def insert_residential_complex_new(
        self, complex_id: str, name: str, city: str = None, district: str = None
    ) -> bool:
        """
        Insert a new residential complex into the database.

        :param complex_id: str, complex ID from Krisha.kz
        :param name: str, complex name
        :param city: str, city name (optional)
        :param district: str, district name (optional)
        :return: bool, True if successful
        """
        self.connect()

        try:
            self.conn.execute(
                """
                INSERT INTO residential_complexes 
                (complex_id, name, city, district) 
                VALUES (?, ?, ?, ?)
            """,
                (complex_id, name, city, district),
            )
            self.conn.commit()
            return True
        except Exception as e:
            logging.error(f"Error inserting residential complex: {e}")
            self.conn.rollback()
            return False
        finally:
            self.disconnect()

    def get_jks_with_unknown_cities(self) -> List[Dict]:
        """
        Get all JKs with NULL or "Unknown" cities.

        :return: List[Dict], list of JK information with keys: complex_id, name
        """
        self.connect()

        try:
            cursor = self.conn.execute(
                """
                SELECT complex_id, name 
                FROM residential_complexes 
                WHERE city IS NULL OR city = 'Unknown'
                ORDER BY name
            """
            )
            jks = [dict(row) for row in cursor.fetchall()]
            return jks
        except Exception as e:
            logging.error(f"Error getting JKs with unknown cities: {e}")
            return []
        finally:
            self.disconnect()

    def update_jk_city(self, complex_id: str, city: str) -> bool:
        """
        Update the city for a specific residential complex.

        :param complex_id: str, complex ID
        :param city: str, city name
        :return: bool, True if successful
        """
        self.connect()

        try:
            self.conn.execute(
                "UPDATE residential_complexes SET city = ? WHERE complex_id = ?",
                (city, complex_id),
            )
            self.conn.commit()
            return True
        except Exception as e:
            logging.error(f"Error updating JK city: {e}")
            self.conn.rollback()
            return False
        finally:
            self.disconnect()


# Convenience functions
def save_rental_flat_to_db(
    flat_info: FlatInfo,
    url: str,
    query_date: str,
    flat_type: str = None,
    db_path: str = "flats.db",
) -> bool:
    """
    Convenience function to save rental flat information to database.

    :param flat_info: FlatInfo, flat information to save
    :param url: str, original URL of the flat
    :param query_date: str, query date (YYYY-MM-DD)
    :param flat_type: str, type of flat ('Studio', '1BR', '2BR', '3BR+')
    :param db_path: str, database file path
    :return: bool, True if successful
    """
    db = OrthancDB(db_path)
    return db.insert_rental_flat(flat_info, url, query_date, flat_type)


def save_sales_flat_to_db(
    flat_info: FlatInfo,
    url: str,
    query_date: str,
    flat_type: str = None,
    db_path: str = "flats.db",
) -> bool:
    """
    Convenience function to save sales flat information to database.

    :param flat_info: FlatInfo, flat information to save
    :param url: str, original URL of the flat
    :param query_date: str, query date (YYYY-MM-DD)
    :param flat_type: str, type of flat ('Studio', '1BR', '2BR', '3BR+')
    :param db_path: str, database file path
    :return: bool, True if successful
    """
    db = OrthancDB(db_path)
    return db.insert_sales_flat(flat_info, url, query_date, flat_type)


def save_exchange_rate_to_db(
    currency: str, rate: float, fetched_at: datetime = None, db_path: str = "flats.db"
) -> bool:
    """
    Convenience function to save exchange rate to database.

    :param currency: str, currency code (EUR, USD, etc.)
    :param rate: float, exchange rate
    :param fetched_at: datetime, timestamp when rate was fetched
    :param db_path: str, database file path
    :return: bool, True if successful
    """
    db = OrthancDB(db_path)
    return db.insert_exchange_rate(currency, rate, fetched_at)


def get_latest_exchange_rate(
    currency: str, db_path: str = "flats.db"
) -> Optional[float]:
    """
    Convenience function to get latest exchange rate.

    :param currency: str, currency code (EUR or USD)
    :param db_path: str, database file path
    :return: Optional[float], latest rate or None if not found
    """
    db = OrthancDB(db_path)
    return db.get_latest_rate(currency)
