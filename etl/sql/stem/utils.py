"""Miscellaneous utility functions for stem-table logic"""

import inspect
import os
from itertools import chain
from typing import Any

from sqlalchemy import case, cast, func, not_
from sqlalchemy.sql import expression
from sqlalchemy.sql.expression import CTE, Case


def get_case_statement(
    column_name: str,
    model: Any,
    cast_as: Any,
    value_type: str = None,
    lookup: Any = None,
) -> Any:
    """
    This function will cast to 'cast_as' all the values of 'column_name' from 'model'
    that have a value_type equal to 'value_type' in the lookup.
    If value type is None it will simply cast to 'cast_as' all the values of 'column_name'.

    The last statement accounts for the cases when you want to cast those
    variables in the lookup labelled as numerical but the column_name in model contains some
    non-numerical values. In this case, the function will return a case statement that
    will return null for those values that are not numerical.
    An example is a lab test that normally contains numerical values but sometimes
    the results is simply "not detected". In this case only the lab results that are actually
    numerical will be casted to FLOAT and the rest will be null.
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

    if value_type == "numerical" and isinstance(exp, Case):
        regex_pattern = r"(\d+(\.\d+)?)|(^\.\d+)"
        exp.whens.insert(
            0,
            (
                not_(
                    func.regexp_matches(
                        getattr(model, column_name), regex_pattern
                    )
                ),
                expression.null(),
            ),
        )
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
    """
    Decorator to toggle the execution of a transform function
    based on the environment variable STEM_TRANSFORMS. If the env var is not set,
    the decorated stem transforms will be executed.
    """

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
