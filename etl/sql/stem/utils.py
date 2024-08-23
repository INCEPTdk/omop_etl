"""Miscellaneous utility functions for stem-table logic"""

import inspect
import os
from itertools import batched, chain
from typing import Any, List, Union

from sqlalchemy import (
    FLOAT,
    TIMESTAMP,
    DateTime,
    String,
    and_,
    case,
    cast,
    func,
    not_,
    select,
)
from sqlalchemy.orm import Mapped
from sqlalchemy.sql import expression
from sqlalchemy.sql.expression import CTE, Case

from ...models.source import SourceModelBase
from ...models.tempmodels import ConceptLookupStem
from ...util.db import AbstractSession

CDM_TIMEZONE: str = "Europe/Copenhagen"


def validate_source_variables(
    session: AbstractSession, model: Any, logger: Any
) -> None:
    """
    Check that the source variables in the Concept Lookup Stem for a given models are present in the source data.
    """
    if hasattr(model, "variable"):
        variable_column = model.variable
    elif hasattr(model, "drug_name"):
        variable_column = model.drug_name
    else:
        return

    missing_vars = session.scalars(
        select(ConceptLookupStem.source_variable)
        .outerjoin(
            model,
            and_(
                variable_column == ConceptLookupStem.source_variable,
            ),
        )
        .where(
            and_(
                variable_column.is_(None),
                ConceptLookupStem.datasource == model.__tablename__,
            )
        )
        .distinct()
    ).all()

    missing_vars_batches = batched(missing_vars, 2)
    for vars_to_print in missing_vars_batches:
        logger.debug(
            "\tMISSING %s source data variables: %s...",
            model.__tablename__.upper(),
            vars_to_print,
        )


def get_batches_from_concept_loopkup_stem(
    model: SourceModelBase,
    session: AbstractSession,
    batch_size: int = None,
    logger: Any = None,
) -> List[int]:
    """Get batches from the ConceptLookupStem table"""
    uids = session.scalars(
        select(ConceptLookupStem.uid).where(
            ConceptLookupStem.datasource == model.__tablename__
        )
    ).all()

    if len(uids) == 0:
        logger.warning(
            "MISSING mapping in concept lookup stem  for %s source data ...",
            model.__tablename__.upper(),
        )

    batch_size = batch_size or len(uids)
    batches = list(batched(uids, batch_size))
    total_batches = sum(len(batch) for batch in batches if batch)

    for batch_count, batch in enumerate(batches):
        logger.debug(
            "\tSTEM Transform %s/%s batch %s is being processed...",
            (batch_count + 1) * len(batch),
            total_batches,
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


def harmonise_timezones(
    column: Mapped[DateTime],
    source_tz: Union[Mapped[String], str],
) -> Case:
    """
    Converts a datetime column to the timezone of the CDM if this differs from
    the source timezone. This happens by localising, converting and
    delocalising the timestamps
    """

    # Localise and convert to the CDM timezone
    in_cdm_tz = func.timezone(CDM_TIMEZONE, func.timezone(source_tz, column))
    return case(
        (source_tz == CDM_TIMEZONE, column),
        else_=cast(in_cdm_tz, TIMESTAMP),  # delocalise to strip timezone part
    )
