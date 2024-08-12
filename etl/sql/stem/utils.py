"""Miscellaneous utility functions for stem-table logic"""

import inspect
import os
from itertools import chain
from typing import Any, List

from sqlalchemy import FLOAT, case, cast, func, not_, select
from sqlalchemy.sql import expression
from sqlalchemy.sql.expression import CTE, Case
from ...models.source import SourceModelBase
from ...util.db import AbstractSession
from ...models.tempmodels import ConceptLookupStem


def get_batches_from_concept_loopkup_stem(
    model: SourceModelBase,
    session: AbstractSession,
    batch_size: int = None,
    logger: Any = None,
) -> List[int]:
    """Get batches from the ConceptLookupStem table"""
    uids = [
        record.uid
        for record in session.query(ConceptLookupStem.uid)
        .where(ConceptLookupStem.datasource == model.__tablename__)
        .all()
    ]

    if batch_size is None:
        batch_size = len(uids)

    if len(uids) == 0:
        logger.warning(
            "MISSING mapping in concept lookup stem  for %s source data ...",
            model.__tablename__.upper(),
        )
        batches = []
    else:
        batches = [
            uids[i : i + batch_size] for i in range(0, len(uids), batch_size)
        ]

    for batch in batches:
        logger.debug(
            "\tSTEM Transform batch %s is being processed...",
            batch,
        )
        yield select(ConceptLookupStem).where(
            ConceptLookupStem.uid.in_(batch)
        ).cte(name="cls_batch")


def try_cast_to_float(
    column: Any,
) -> Case:
    """
    Args:
    column: column to be casted to float (usually passed as attribute of a model)
    Returns:
    stmt: case statement that will cast to float when the regexp pattern is matched
          and return null otherwise
    """
    regex_pattern = (
        r"^[+-]?([0-9]*\.[0-9]+|[0-9]+(\.[0-9]*)?)([eE][+-]?[0-9]+)?$"
    )
    stmt = case(
        (
            not_(func.regexp_matches(column, regex_pattern)),
            expression.null(),
        ),
        else_=cast(column, FLOAT),
    )
    return stmt


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

    if cast_as == FLOAT and isinstance(exp, Case):
        try_cast_stmt = try_cast_to_float(getattr(model, column_name))
        exp.whens.insert(0, try_cast_stmt.whens[0])
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
