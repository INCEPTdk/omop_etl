"""Miscellaneous utility functions for stem-table logic"""

import inspect
import os
import re
from datetime import timedelta
from itertools import chain
from typing import Any

from sqlalchemy import case, cast
from sqlalchemy.sql import expression
from sqlalchemy.sql.expression import CTE

from etl.util.exceptions import InvalidEraLookbackInterval


def get_case_statement(
    column_name: str,
    model: Any,
    cast_as: Any,
    value_type: str = None,
    lookup: Any = None,
) -> Any:
    """
    This is a general function that given a column it returns:
        None if the name of that column has not been specified in the lookup.
        The columns with the specific casts.
        If value type is specified, allows to case on the value_type in the lookup.
    """

    if isinstance(model, CTE):
        model = model.c  # make getattr() to look in the right place for CTEs

    if column_name is None:
        exp = cast(expression.null(), cast_as)
    elif value_type:
        assert (
            lookup is not None
        ), "Lookup must be provided if value_type is specified."
        exp = case(
            (
                lookup.value_type == value_type,
                cast(
                    getattr(model, column_name),
                    cast_as,
                ),
            ),
            else_=expression.null(),
        )
    else:
        exp = cast(getattr(model, column_name), cast_as)
    return exp


def find_unique_column_names(
    session: Any = None,
    model: Any = None,
    lookup_model: Any = None,
    column: str = None,
) -> Any:
    """
    For a given column ad datasource, ie start_date and
    procedures, find all the names of that column in the source.
    If there is more than one name for that column/data source combination,
    raise a warning as this has not been handled yet.
    """
    lst = (
        session.query(getattr(lookup_model, column))
        .where(lookup_model.datasource == model.__tablename__)
        .distinct()
        .all()
    )

    col_set = set(x for x in chain(*lst) if x is not None)

    if len(col_set) == 0:
        col_name = None
    elif len(col_set) == 1:
        col_name = col_set.pop()
    else:
        raise NotImplementedError(
            f"""More than one unique value found.
            Within one single datasource there shouln't be more than one {column}."""
        )
    return col_name


def toggle_stem_transform(transform_function):
    caller_module = inspect.getmodule(inspect.stack()[1][0])
    # pylint: disable=unused-variable
    transform_name, suffix = os.path.splitext(
        os.path.basename(caller_module.__file__)
    )

    active_transforms = set(
        os.getenv("STEM_TRANSFORMS", default=transform_name).split(",")
    )

    def wrapper(*args, **kwargs):
        if transform_name in active_transforms:
            return transform_function(*args, **kwargs)
        return "SELECT NULL;"

    return wrapper


def parse_interval(interval: str = None):
    try:
        match = re.match(
            r"(\d+)\s*(hour|day|minute|second)s?", interval, re.IGNORECASE
        )
        value, unit = match.groups()
        unit = unit if unit.endswith("s") else unit + "s"
        return timedelta(**{unit: float(value)})
    except (TypeError, ValueError):
        raise InvalidEraLookbackInterval(interval)
