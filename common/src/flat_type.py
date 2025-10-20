"""
Flat type definitions.

Enum of supported flat types to be reused across the project.
"""

from enum import Enum


class FlatType(str, Enum):
    """
    Enumeration of flat types.
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


def normalize_flat_type(value: str) -> str:
    """
    Normalize arbitrary flat type string to a canonical FlatType value when possible.
    :param value: str, incoming flat type string
    :return: str, normalized FlatType string value
    """
    if not value:
        return FlatType.ONE_BEDROOM.value
    v = value.strip().lower()
    if v in {"studio", "студия"}:
        return FlatType.STUDIO.value
    if v in {"1br", "1 br", "1-bedroom", "one-bedroom", "1-комнатная", "1 комнатная"}:
        return FlatType.ONE_BEDROOM.value
    if v in {"2br", "2 br", "2-bedroom", "two-bedroom", "2-комнатная", "2 комнатная"}:
        return FlatType.TWO_BEDROOM.value
    return FlatType.THREE_PLUS_BEDROOM.value
