from enum import Enum


class FlatType(Enum):
    """
    Enum for flat types (Studio, 1BR, 2BR, etc.).
    """
    STUDIO = "Studio"
    ONE_BEDROOM = "1BR"
    TWO_BEDROOM = "2BR"
    THREE_PLUS_BEDROOM = "3BR+"

FLAT_TYPES = [FlatType.STUDIO,
              FlatType.ONE_BEDROOM,
              FlatType.TWO_BEDROOM,
              FlatType.THREE_PLUS_BEDROOM]

FLAT_TYPE_VALUES = [FlatType.STUDIO.value,
                    FlatType.ONE_BEDROOM.value,
                    FlatType.TWO_BEDROOM.value,
                    FlatType.THREE_PLUS_BEDROOM.value]