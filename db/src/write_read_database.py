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

    # Rental flats operations
    def insert_rental_flat(
        self, flat_info: FlatInfo, url: str, query_date: str, flat_type: str = None
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
                    construction_year, parking, description, url, query_date
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
        self, flat_info: FlatInfo, url: str, query_date: str, flat_type: str = None
    ) -> bool:
        """
        Insert sales flat information into database.

        :param flat_info: FlatInfo, flat information to store
        :param url: str, original URL of the flat
        :param query_date: str, date when the query was made (YYYY-MM-DD)
        :param flat_type: str, type of flat ('Studio', '1BR', '2BR', '3BR+') - optional, uses flat_info.flat_type if not provided
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
                    construction_year, parking, description, url, query_date
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
        self, flat_info: FlatInfo, url: str, query_date: str, flat_type: str = None
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
                description = ?, url = ?, updated_at = CURRENT_TIMESTAMP
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
                flat_info.flat_id,
                query_date,
            ),
        )

        self.conn.commit()
        self.disconnect()
        return True

    def update_sales_flat(
        self, flat_info: FlatInfo, url: str, query_date: str, flat_type: str = None
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
                description = ?, url = ?, updated_at = CURRENT_TIMESTAMP
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
                       query_date, scraped_at
                FROM rental_flats 
                WHERE residential_complex LIKE ?
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
                    )

            flats.extend(list(rental_data.values()))

        if sales_or_rentals in ["sales", "both"]:
            cursor = self.conn.execute(
                """
                SELECT DISTINCT flat_id, price, area, flat_type, residential_complex, floor, 
                       total_floors, construction_year, parking, description, url, 
                       query_date, scraped_at
                FROM sales_flats 
                WHERE residential_complex LIKE ?
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

        :return: List[dict], list of favorite flats with current data
        """
        self.connect()

        # Get rental favorites with current data
        rental_favorites = self.conn.execute("""
            SELECT 
                f.flat_id, f.flat_type, f.added_at, f.notes,
                rf.price, rf.area, rf.residential_complex, rf.floor,
                rf.total_floors, rf.construction_year, rf.parking,
                rf.description, rf.url, rf.scraped_at
            FROM favorites f
            JOIN rental_flats rf ON f.flat_id = rf.flat_id
            WHERE f.flat_type = 'rental'
            ORDER BY f.added_at DESC
        """).fetchall()

        # Get sales favorites with current data
        sales_favorites = self.conn.execute("""
            SELECT 
                f.flat_id, f.flat_type, f.added_at, f.notes,
                sf.price, sf.area, sf.residential_complex, sf.floor,
                sf.total_floors, sf.construction_year, sf.parking,
                sf.description, sf.url, sf.scraped_at
            FROM favorites f
            JOIN sales_flats sf ON f.flat_id = sf.flat_id
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
                       construction_year, parking, description, url, query_date
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
                       construction_year, parking, description, url, query_date
                FROM rental_flats 
                WHERE flat_id = ?
            """,
                (flat_id,),
            )

        flat_data = cursor.fetchone()
        if not flat_data:
            logging.info(f"Flat {flat_id} not found in source table")
            return False

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
            WHERE rf.residential_complex = ?
        """

        cursor = self.conn.execute(query, (jk_name,))
        latest_rentals = [dict(row) for row in cursor.fetchall() if row["rn"] == 1]

        self.disconnect()
        return latest_rentals

    def get_latest_sales_for_jk(self, jk_name: str) -> List[dict]:
        """
        Get latest sales data for a residential complex (most recent query_date for each flat_id).

        :param jk_name: str, name of the residential complex
        :return: List[dict], list of latest sales data with all fields
        """
        self.connect()

        query = """
            SELECT sf.*,
                   ROW_NUMBER() OVER (PARTITION BY sf.flat_id ORDER BY sf.query_date DESC) as rn
            FROM sales_flats sf
            WHERE sf.residential_complex = ?
        """

        cursor = self.conn.execute(query, (jk_name,))
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
