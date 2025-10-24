"""
Unit tests for database operations using pytest.

Tests write, read, and delete operations for both rental and sales flats.

pytest db/test/test_flats_database_operations.py -v
"""

import pytest
import os
from datetime import datetime
from typing import Dict, Any

from db.src.write_read_database import OrthancDB
from common.src.flat_info import FlatInfo


class TestDatabaseOperations:
    """Test class for database operations."""
    
    @pytest.fixture
    def db(self):
        """Create database connection using actual database."""
        return OrthancDB("flats.db")
    
    @pytest.fixture
    def sample_rental_flat(self):
        """Create a sample rental flat for testing."""
        return FlatInfo(
            flat_id="test_rental_321",
            price=150000,  # 150k tenge per month
            area=45.5,
            flat_type="1BR",
            residential_complex="Test Complex",
            floor=5,
            total_floors=12,
            construction_year=2020,
            parking="Подземная парковка",
            description="Красивая 1-комнатная квартира в новом доме",
            is_rental=True
        )
    
    @pytest.fixture
    def sample_sales_flat(self):
        """Create a sample sales flat for testing."""
        return FlatInfo(
            flat_id="test_sales_654",
            price=25000000,  # 25M tenge
            area=75.0,
            flat_type="2BR",
            residential_complex="Test Complex",
            floor=8,
            total_floors=15,
            construction_year=2019,
            parking="Открытая парковка",
            description="Просторная 2-комнатная квартира с балконом",
            is_rental=False
        )
    
    def test_rental_flat_crud_operations(self, db, sample_rental_flat):
        """Test complete CRUD operations for rental flats."""
        # Test data
        url = "https://krisha.kz/a/show/test_rental_123"
        query_date = datetime.now().strftime('%Y-%m-%d')

        # 1. INSERT - Test inserting rental flat
        success = db.insert_rental_flat(
            sample_rental_flat,
            url,
            query_date,
            sample_rental_flat.flat_type
        )
        assert success, "Failed to insert rental flat"
        
        # 2. READ - Test reading the inserted flat
        rental_flats = db.get_rental_flats_by_date(query_date)
        assert len(rental_flats) > 0, "No rental flats found after insertion"

        # Find our specific flat
        inserted_flat = None
        for flat in rental_flats:
            if flat.flat_id == sample_rental_flat.flat_id:
                inserted_flat = flat
                break

        assert inserted_flat is not None, "Inserted rental flat not found"

        # Verify all fields match
        assert inserted_flat.flat_id == sample_rental_flat.flat_id
        assert inserted_flat.price == sample_rental_flat.price
        assert inserted_flat.area == sample_rental_flat.area
        assert inserted_flat.flat_type == sample_rental_flat.flat_type
        assert inserted_flat.residential_complex == sample_rental_flat.residential_complex
        assert inserted_flat.floor == sample_rental_flat.floor
        assert inserted_flat.total_floors == sample_rental_flat.total_floors
        assert inserted_flat.construction_year == sample_rental_flat.construction_year
        assert inserted_flat.parking == sample_rental_flat.parking
        assert inserted_flat.description == sample_rental_flat.description

        # 3. UPDATE - Test updating the flat
        updated_flat = FlatInfo(
            flat_id=sample_rental_flat.flat_id,
            price=160000,  # Updated price
            area=sample_rental_flat.area,
            flat_type=sample_rental_flat.flat_type,
            residential_complex=sample_rental_flat.residential_complex,
            floor=sample_rental_flat.floor,
            total_floors=sample_rental_flat.total_floors,
            construction_year=sample_rental_flat.construction_year,
            parking=sample_rental_flat.parking,
            description="Updated description for rental flat",
            is_rental=True
        )

        update_success = db.update_rental_flat(updated_flat, url, query_date, updated_flat.flat_type)
        assert update_success, "Failed to update rental flat"

        # Verify update
        updated_flats = db.get_rental_flats_by_date(query_date)
        updated_flat_data = None
        for flat in updated_flats:
            if flat.flat_id == sample_rental_flat.flat_id:
                updated_flat_data = flat
                break

        assert updated_flat_data is not None, "Updated rental flat not found"
        assert updated_flat_data.price == 160000, "Price not updated correctly"
        assert updated_flat_data.description == "Updated description for rental flat", "Description not updated correctly"

        # 4. DELETE - Test deleting the flat
        db.connect()
        cursor = db.conn.execute(
            "DELETE FROM rental_flats WHERE flat_id = ? AND query_date = ?",
            (sample_rental_flat.flat_id, query_date)
        )
        db.conn.commit()
        db.disconnect()

        # Verify deletion
        remaining_flats = db.get_rental_flats_by_date(query_date)
        flat_exists = any(flat.flat_id == sample_rental_flat.flat_id for flat in remaining_flats)
        assert not flat_exists, "Rental flat was not deleted"
    
    def test_sales_flat_crud_operations(self, db, sample_sales_flat):
        """Test complete CRUD operations for sales flats."""
        # Test data
        url = "https://krisha.kz/a/show/test_sales_456"
        query_date = datetime.now().strftime('%Y-%m-%d')
        
        # 1. INSERT - Test inserting sales flat
        success = db.insert_sales_flat(
            sample_sales_flat, 
            url, 
            query_date, 
            sample_sales_flat.flat_type
        )
        assert success, "Failed to insert sales flat"
        
        # 2. READ - Test reading the inserted flat
        sales_flats = db.get_sales_flats_by_date(query_date)
        assert len(sales_flats) > 0, "No sales flats found after insertion"
        
        # Find our specific flat
        inserted_flat = None
        for flat in sales_flats:
            if flat.flat_id == sample_sales_flat.flat_id:
                inserted_flat = flat
                break
        
        assert inserted_flat is not None, "Inserted sales flat not found"
        
        # Verify all fields match
        assert inserted_flat.flat_id == sample_sales_flat.flat_id
        assert inserted_flat.price == sample_sales_flat.price
        assert inserted_flat.area == sample_sales_flat.area
        assert inserted_flat.flat_type == sample_sales_flat.flat_type
        assert inserted_flat.residential_complex == sample_sales_flat.residential_complex
        assert inserted_flat.floor == sample_sales_flat.floor
        assert inserted_flat.total_floors == sample_sales_flat.total_floors
        assert inserted_flat.construction_year == sample_sales_flat.construction_year
        assert inserted_flat.parking == sample_sales_flat.parking
        assert inserted_flat.description == sample_sales_flat.description
        
        # 3. UPDATE - Test updating the flat
        updated_flat = FlatInfo(
            flat_id=sample_sales_flat.flat_id,
            price=26000000,  # Updated price
            area=sample_sales_flat.area,
            flat_type=sample_sales_flat.flat_type,
            residential_complex=sample_sales_flat.residential_complex,
            floor=sample_sales_flat.floor,
            total_floors=sample_sales_flat.total_floors,
            construction_year=sample_sales_flat.construction_year,
            parking=sample_sales_flat.parking,
            description="Updated description for sales flat",
            is_rental=False
        )
        
        update_success = db.update_sales_flat(updated_flat, url, query_date, updated_flat.flat_type)
        assert update_success, "Failed to update sales flat"
        
        # Verify update
        updated_flats = db.get_sales_flats_by_date(query_date)
        updated_flat_data = None
        for flat in updated_flats:
            if flat.flat_id == sample_sales_flat.flat_id:
                updated_flat_data = flat
                break
        
        assert updated_flat_data is not None, "Updated sales flat not found"
        assert updated_flat_data.price == 26000000, "Price not updated correctly"
        assert updated_flat_data.description == "Updated description for sales flat", "Description not updated correctly"
        
        # 4. DELETE - Test deleting the flat
        db.connect()
        cursor = db.conn.execute(
            "DELETE FROM sales_flats WHERE flat_id = ? AND query_date = ?",
            (sample_sales_flat.flat_id, query_date)
        )
        db.conn.commit()
        db.disconnect()
        
        # Verify deletion
        remaining_flats = db.get_sales_flats_by_date(query_date)
        flat_exists = any(flat.flat_id == sample_sales_flat.flat_id for flat in remaining_flats)
        assert not flat_exists, "Sales flat was not deleted"
    
    def test_flat_type_classification(self, db):
        """Test flat type classification and storage."""
        # Test different flat types
        test_cases = [
            ("studio_test", 25.0, "Studio", "Студия в центре города"),
            ("onebr_test", 45.0, "1BR", "1-комнатная квартира"),
            ("twobr_test", 65.0, "2BR", "2-комнатная квартира"),
            ("threebr_test", 95.0, "3BR+", "3-комнатная квартира"),
        ]
        
        query_date = datetime.now().strftime('%Y-%m-%d')
        
        for flat_id, area, expected_type, description in test_cases:
            # Create test flat
            test_flat = FlatInfo(
                flat_id=flat_id,
                price=100000,
                area=area,
                flat_type=expected_type,
                residential_complex="Test Complex",
                floor=1,
                total_floors=5,
                construction_year=2020,
                parking="Да",
                description=description,
                is_rental=True
            )
            
            # Insert rental flat
            success = db.insert_rental_flat(test_flat, f"https://krisha.kz/a/show/{flat_id}", query_date, expected_type)
            assert success, f"Failed to insert {expected_type} flat"
            
            # Verify flat type is stored correctly
            rental_flats = db.get_rental_flats_by_date(query_date)
            inserted_flat = next((flat for flat in rental_flats if flat.flat_id == flat_id), None)
            assert inserted_flat is not None, f"Flat {flat_id} not found"
            assert inserted_flat.flat_type == expected_type, f"Flat type mismatch for {flat_id}: expected {expected_type}, got {inserted_flat.flat_type}"
            
            # Clean up - delete the test flat
            db.connect()
            db.conn.execute("DELETE FROM rental_flats WHERE flat_id = ? AND query_date = ?", (flat_id, query_date))
            db.conn.commit()
            db.disconnect()
    
    def test_historical_statistics(self, db):
        """Test historical statistics generation."""
        # Insert some test data
        query_date = datetime.now().strftime('%Y-%m-%d')
        
        # Insert rental flats
        rental_flats = [
            FlatInfo("rental_historical_stats_1", 100000, 40.0, "1BR", "Test Complex historical stats", 1, 5, 2020, "Да", "1BR", True),
            FlatInfo("rental_historical_stats_2", 120000, 50.0, "1BR", "Test Complex historical stats", 2, 5, 2020, "Да", "1BR", True),
            FlatInfo("rental_historical_stats_3", 150000, 60.0, "2BR", "Test Complex historical stats", 3, 5, 2020, "Да", "2BR", True),
        ]
        
        for flat in rental_flats:
            db.insert_rental_flat(flat, f"https://krisha.kz/a/show/{flat.flat_id}", query_date, flat.flat_type)
        
        # Insert sales flats
        sales_flats = [
            FlatInfo("sales_historical_stats_1", 10000000, 40.0, "1BR", "Test Complex historical stats", 1, 5, 2020, "Да", "1BR", False),
            FlatInfo("sales_historical_stats_2", 12000000, 50.0, "1BR", "Test Complex historical stats", 2, 5, 2020, "Да", "1BR", False),
            FlatInfo("sales_historical_stats_3", 15000000, 60.0, "2BR", "Test Complex historical stats", 3, 5, 2020, "Да", "2BR", False),
        ]
        
        for flat in sales_flats:
            db.insert_sales_flat(flat, f"https://krisha.kz/a/show/{flat.flat_id}", query_date, flat.flat_type)
        
        # Test historical statistics
        stats = db.get_historical_statistics(query_date, query_date, "Test Complex historical stats")
        
        assert stats['rental_stats']['total_rentals'] ==3, "Incorrect rental count"
        assert stats['sales_stats']['total_sales'] == 3, "Incorrect sales count"
        assert stats['rental_stats']['min_rental_price'] == 100000, "Incorrect min rental price"
        assert stats['rental_stats']['max_rental_price'] == 150000, "Incorrect max rental price"
        assert stats['sales_stats']['min_sales_price'] == 10000000, "Incorrect min sales price"
        assert stats['sales_stats']['max_sales_price'] == 15000000, "Incorrect max sales price"
        
        # Clean up - delete test data
        db.connect()
        for flat in rental_flats + sales_flats:
            db.conn.execute("DELETE FROM rental_flats WHERE flat_id = ? AND query_date = ?", (flat.flat_id, query_date))
            db.conn.execute("DELETE FROM sales_flats WHERE flat_id = ? AND query_date = ?", (flat.flat_id, query_date))
        db.conn.commit()
        db.disconnect()
    
    def test_jk_performance_snapshot(self, db):
        """Test JK performance snapshot creation."""
        # Insert test data for a specific complex
        query_date = datetime.now().strftime('%Y-%m-%d')
        residential_complex_name = "Test Performance Complex"
        
        # Insert rental data
        rental_flat = FlatInfo(
            "perf_snap_rental_1", 100000, 50.0, "1BR", residential_complex_name, 1, 5, 2020, "Да", "Test rental", True
        )
        db.insert_rental_flat(rental_flat, "https://krisha.kz/a/show/perf_rental_1", query_date, "1BR")
        
        # Insert sales data
        sales_flat = FlatInfo(
            "perf_snap_sales_1", 10000000, 50.0, "1BR", residential_complex_name, 1, 5, 2020, "Да", "Test sales", False
        )
        db.insert_sales_flat(sales_flat, "https://krisha.kz/a/show/perf_sales_1", query_date, "1BR")
        
        # Create performance snapshot
        success = db.create_jk_performance_snapshot(residential_complex_name, query_date)
        assert success, "Failed to create performance snapshot"
        
        # Verify snapshot was created
        snapshots = db.get_jk_performance_snapshots(residential_complex=residential_complex_name)
        assert len(snapshots) > 0, "No performance snapshots found"
        
        snapshot = snapshots[0]
        assert snapshot['residential_complex'] == residential_complex_name, "Incorrect complex name in snapshot"
        assert snapshot['snapshot_date'] == query_date, "Incorrect snapshot date"
        assert snapshot['total_rental_flats'] == 1, "Incorrect rental count in snapshot"
        assert snapshot['total_sales_flats'] == 1, "Incorrect sales count in snapshot"
        assert snapshot['onebr_rental_count'] == 1, "Incorrect 1BR rental count"
        assert snapshot['onebr_sales_count'] == 1, "Incorrect 1BR sales count"
        
        # Clean up - delete test data
        db.connect()
        db.conn.execute("DELETE FROM rental_flats WHERE flat_id = ? AND query_date = ?", (rental_flat.flat_id, query_date))
        db.conn.execute("DELETE FROM sales_flats WHERE flat_id = ? AND query_date = ?", (sales_flat.flat_id, query_date))
        db.conn.execute("DELETE FROM jk_performance_snapshots WHERE residential_complex = ? AND snapshot_date = ?", (residential_complex_name, query_date))
        db.conn.commit()
        db.disconnect()