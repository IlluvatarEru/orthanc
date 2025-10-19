from webapp import get_similar_properties, load_analysis_config
from db.src.flat_info_from_db import get_flat_info
import logging

def test_flat():
    flat_id = "1004177142"
    analysis_config = load_analysis_config("../../config/src/config.toml")
    db_path = "../../flats.db"
    flat_info = get_flat_info(flat_id, flash_to_frontend=False, db_path=db_path)
    similar_rentals, similar_sales = get_similar_properties(flat_info, area_tolerance=100,db_path=db_path)
    logging.info(len(similar_rentals))
    logging.info(len(similar_sales))

if __name__ == "__main__":
    test_flat()