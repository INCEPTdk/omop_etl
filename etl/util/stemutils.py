"""Miscellaneous utility functions for stem-table logic"""

from itertools import chain
from typing import Any

from sqlalchemy import DateTime


def flatten_to_set(lst: list = None) -> set:
    """Flatten a list of iterables and return a set of unique values"""

    return set(x for x in chain(*lst) if x is not None)


def find_datetime_columns(model: Any = None) -> list:
    """Find datetime columns in a model"""

    return [
        c.name for c in model.__table__.columns if isinstance(c.type, DateTime)
    ]
