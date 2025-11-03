from db.src.write_read_database import OrthancDB
import pytest


class TestBlackListManagement:
    @pytest.fixture
    def db(self):
        """Create database connection using actual database."""
        return OrthancDB("flats.db")

    def test_blacklist_management(self, db):
        jk = "Meridian Apartments"
        if db.is_jk_blacklisted(name=jk):
            success = db.whitelist_jk_by_name(jk)
            assert success

        success = db.blacklist_jk_by_name(jk)
        assert success

        success = db.whitelist_jk_by_name(jk)
        assert success
