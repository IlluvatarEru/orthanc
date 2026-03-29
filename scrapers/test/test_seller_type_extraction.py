"""
Unit tests for seller type extraction functions.

pytest scrapers/test/test_seller_type_extraction.py -v
"""

from scrapers.src.utils import (
    extract_seller_type_from_api,
    extract_seller_name_from_api,
)


class TestExtractSellerTypeFromApi:
    """Tests for extract_seller_type_from_api."""

    def test_owner(self):
        advert = {"owner": {"isOwner": True, "isPro": False}}
        assert extract_seller_type_from_api(advert) == "owner"

    def test_agent(self):
        advert = {"owner": {"isPro": True, "isOwner": False}}
        assert extract_seller_type_from_api(advert) == "agent"

    def test_builder(self):
        advert = {"owner": {"isBuilder": True}}
        assert extract_seller_type_from_api(advert) == "builder"

    def test_complex(self):
        advert = {"owner": {"isComplex": True}}
        assert extract_seller_type_from_api(advert) == "complex"

    def test_empty_owner(self):
        advert = {"owner": {}}
        assert extract_seller_type_from_api(advert) is None

    def test_no_owner_key(self):
        advert = {}
        assert extract_seller_type_from_api(advert) is None

    def test_label_fallback_specialist(self):
        advert = {
            "owner": {
                "isPro": False,
                "isOwner": False,
                "label": {"name": "identified-specialist", "title": "Specialist"},
            }
        }
        assert extract_seller_type_from_api(advert) == "agent"

    def test_priority_owner_over_pro(self):
        """isOwner takes priority over isPro."""
        advert = {"owner": {"isOwner": True, "isPro": True}}
        assert extract_seller_type_from_api(advert) == "owner"

    def test_priority_builder_over_pro(self):
        """isBuilder takes priority over isPro."""
        advert = {"owner": {"isBuilder": True, "isPro": True}}
        assert extract_seller_type_from_api(advert) == "builder"


class TestExtractSellerNameFromApi:
    """Tests for extract_seller_name_from_api."""

    def test_name_with_company(self):
        advert = {"owner": {"title": "Иванов Иван, компания «Недвижимость»"}}
        assert (
            extract_seller_name_from_api(advert)
            == "Иванов Иван, компания «Недвижимость»"
        )

    def test_name_only(self):
        advert = {"owner": {"title": "Иванов Иван"}}
        assert extract_seller_name_from_api(advert) == "Иванов Иван"

    def test_empty_title(self):
        advert = {"owner": {"title": ""}}
        assert extract_seller_name_from_api(advert) is None

    def test_no_title_key(self):
        advert = {"owner": {"isPro": True}}
        assert extract_seller_name_from_api(advert) is None

    def test_no_owner_key(self):
        advert = {}
        assert extract_seller_name_from_api(advert) is None
