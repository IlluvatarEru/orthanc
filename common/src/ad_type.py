from enum import Enum


class AdType(Enum):
    """
    Enum for advertisement types (rental vs sales).
    """

    RENTAL = "rental"
    SALES = "sales"
    SALE = "sale"  # Alternative spelling used in some places
