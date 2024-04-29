"""Miscellaneous utility functions for stem-table logic"""

from itertools import chain
from typing import Any

from sqlalchemy import case, cast
from sqlalchemy.sql import expression


def get_case_statement(
    column_name: str,
    model: Any = None,
    cast_as: Any = None,
    value_type: str = None,
    lookup: Any = None,
) -> Any:
    """
    This is a general function that given a column it returns:
        None if the name of that column has not been specified in the lookup.
        The columns with the specific casts.
        If value type is specified, allows to case on the value_type in the lookup.
    """

    if column_name is None:
        exp = expression.null()
    elif value_type:
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
) -> list:
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
