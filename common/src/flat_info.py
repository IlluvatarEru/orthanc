from dataclasses import dataclass
from typing import Optional


@dataclass
class FlatInfo:
    """
    Data class to store flat information extracted from Krisha.kz.

    :param flat_id: str, unique identifier of the flat from URL
    :param price: int, price in tenge
    :param area: float, area in square meters
    :param residential_complex: Optional[str], name of the residential complex (JK)
    :param floor: Optional[int], floor number where the flat is located
    :param total_floors: Optional[int], total number of floors in the building
    :param construction_year: Optional[int], year of construction
    :param parking: Optional[str], parking information
    :param description: str, full description text
    :param is_rental: bool, True if the flat is for rent, False if for sale
    :param flat_type: Optional[str], type of flat ('Studio', '1BR', '2BR', '3BR+')
    :param archived: bool, True if the flat is archived, False otherwise
    """

    flat_id: str
    price: int
    area: float
    flat_type: str
    residential_complex: Optional[str]
    floor: Optional[int]
    total_floors: Optional[int]
    construction_year: Optional[int]
    parking: Optional[str]
    description: str
    is_rental: bool = False
    archived: bool = False
    scraped_at: Optional[str] = None
    city: Optional[str] = None
