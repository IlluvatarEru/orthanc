"""
Test blacklist management functionality.

pytest db/test/test_blacklist_management.py -v
"""

import pytest
import tempfile
import os

from db.src.write_read_database import OrthancDB


class TestBlackListManagement:
    """Test class for blacklist management operations."""

    @pytest.fixture
    def db(self):
        """Create database connection using a temporary test database."""
        # Create a temporary database file for testing
        fd, temp_db_path = tempfile.mkstemp(suffix=".db")
        os.close(fd)

        db = OrthancDB(temp_db_path)

        # Insert a test JK into residential_complexes table for blacklist testing
        db.connect()
        db.conn.execute(
            """
            INSERT INTO residential_complexes (complex_id, name, city, district)
            VALUES (?, ?, ?, ?)
        """,
            ("test_jk_123", "Test Complex", "Almaty", "Bostandyk"),
        )
        db.conn.commit()
        db.disconnect()

        yield db

        # Cleanup: remove the temporary database after test
        db.disconnect()
        if os.path.exists(temp_db_path):
            os.remove(temp_db_path)

    def test_blacklist_and_whitelist_jk(self, db):
        """Test blacklisting and whitelisting a JK."""
        jk_name = "Test Complex"

        # Initially should not be blacklisted
        assert not db.is_jk_blacklisted(name=jk_name), (
            "JK should not be blacklisted initially"
        )

        # Blacklist the JK
        success = db.blacklist_jk_by_name(jk_name)
        assert success, "Failed to blacklist JK"

        # Verify it's now blacklisted
        assert db.is_jk_blacklisted(name=jk_name), (
            "JK should be blacklisted after blacklist operation"
        )

        # Whitelist the JK
        success = db.whitelist_jk_by_name(jk_name)
        assert success, "Failed to whitelist JK"

        # Verify it's no longer blacklisted
        assert not db.is_jk_blacklisted(name=jk_name), (
            "JK should not be blacklisted after whitelist operation"
        )

    def test_get_blacklisted_jks(self, db):
        """Test getting all blacklisted JKs."""
        jk_name = "Test Complex"

        # Initially no blacklisted JKs
        blacklisted = db.get_blacklisted_jks()
        assert len(blacklisted) == 0, "Should have no blacklisted JKs initially"

        # Blacklist the JK
        db.blacklist_jk_by_name(jk_name)

        # Verify it appears in the blacklist
        blacklisted = db.get_blacklisted_jks()
        assert len(blacklisted) == 1, "Should have 1 blacklisted JK"
        assert blacklisted[0]["name"] == jk_name, "Blacklisted JK should match"

    def test_is_jk_blacklisted_with_invalid_input(self, db):
        """Test is_jk_blacklisted with invalid input."""
        # No identifier provided
        result = db.is_jk_blacklisted()
        assert result is False, "Should return False when no identifier provided"

        # Non-existent JK
        result = db.is_jk_blacklisted(name="NonExistentJK")
        assert result is False, "Should return False for non-existent JK"
