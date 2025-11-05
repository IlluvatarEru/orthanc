from scrapers.src.utils import extract_jk_from_description

import logging


class TestUtils:
    """Test class for rental flat scraping functionality."""

    def test_extract_jk_from_description_meridian(self):
        description = """
        жил. комплекс Meridian Apartments, монолитный дом, 2024 г.п., состояние: черновая отделка, потолки 3м., 🏡🔥 Продаётся стильная студия 35 м² в ЖК &quot;Meridian Apartments&quot;** ✨ Параметры квартиры:…
        """
        jk = extract_jk_from_description(description)
        assert jk is not None
        logging.info("\n---------\n")
        logging.info(f"jk={jk}")
        assert "meridian" in jk.lower()

    def test_extract_jk_from_description_turcyn(self):
        description = """
        жил. комплекс Турсын Астана – 2, меблирована полностью, Срочно сдам!        
        """
        jk = extract_jk_from_description(description)
        assert jk is not None
        logging.info("\n---------\n")
        logging.info(f"jk={jk}")
