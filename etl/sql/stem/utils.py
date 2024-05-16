"""Miscellaneous utility functions for stem-table logic"""

from itertools import chain
from typing import Any, Dict, Final

from sqlalchemy import and_, case, cast, func
from sqlalchemy.sql import expression
from sqlalchemy.sql.expression import CTE

from ...models.source import Prescriptions


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


def get_conversion_factor(
    CteAdministrations: Any = None,
    CtePrescriptions: Any = None,
    drug_name: str = None,
) -> Any:
    CONVERSIONS: Final[Dict[str, Any]] = {
        "noradrenalinsad": case(
            (CtePrescriptions.c.epaspresdrugunit == "ug", 0.001),
            else_=1,
        ),
        "g_to_mg": case(
            (CtePrescriptions.c.epaspresdrugunit == "g", 1000),
            else_=1,
        ),
        "vancomycin1g": case(
            (
                and_(
                    CtePrescriptions.c.epaspresdrugunit == "g",
                    CtePrescriptions.c.epaspresdose == 0.0,
                    CteAdministrations.c.administration_type == "discrete",
                ),
                1000,
            ),
            else_=1,
        ),
        "metaoxedrinsad": case(
            (CtePrescriptions.c.epaspresdrugunit == "ug", 0.001),
            (
                CtePrescriptions.c.epaspresdrugunit == "ml",
                CtePrescriptions.c.epaspresconc,
            ),
            else_=1,
        ),
        "corotropsnf": 0.001,
        "kaliumkloridps": 75,
        "kaliumkloridsad": 75,
        "minirinfrr": 1000,
        "novorapidiu": 1,
        "desmopressintv": 1,
    }

    return CONVERSIONS.get(drug_name, 1.0)


def get_bolus_quantity_recipe(
    CteAdministrations: Any = None,
    CtePrescriptions: Any = None,
    drug_name: str = None,
) -> Any:
    """
    Takes CTEs of administrations and prescriptions plus an administration type
    and a drug name and returns the quantity recipe for administrations.
    The CTEs are usually subsets of the full administrations and prescriptions
    tables.
    """

    RECIPES: Final[Dict[str, Any]] = {
        "noradrenalinsad": func.coalesce(
            CteAdministrations.c.value0,
            CteAdministrations.c.value
            * case(
                (  # weight-based dose
                    CtePrescriptions.c.epaspresdose == 0.0,
                    0.6 * CtePrescriptions.c.epaspresweight / 1000,
                ),
                (  # assume disolved in 100 mL
                    CtePrescriptions.c.epaspresmixamount == 0.0,
                    CtePrescriptions.c.epaspresdose / 100,
                ),
                else_=CtePrescriptions.c.epaspresdose
                / CtePrescriptions.c.epaspresmixamount,
            ),
        ),
        "solumdr": func.coalesce(
            CteAdministrations.c.value0, CteAdministrations.c.value
        ),
    }

    return RECIPES.get(drug_name, None)


def get_continuous_quantity_recipe(
    CteAdministrations: Any = None,
    CtePrescriptions: Any = None,
    drug_name: str = None,
) -> Any:
    """
    Takes CTEs of administrations and prescriptions plus an administration type
    and a drug name and returns the quantity recipe for administrations.
    The CTEs are usually subsets of the full administrations and prescriptions
    tables.
    """

    RECIPES: Final[Dict[str, Any]] = {
        "metaoxedrinsad": CteAdministrations.c.value
        * func.coalesce(
            CteAdministrations.c.value / CteAdministrations.c.value1,
            CtePrescriptions.c.epaspresconc,
        ),
        "noradrenalinsad": func.coalesce(
            CteAdministrations.c.value0,
            CteAdministrations.c.value
            * case(
                (
                    CtePrescriptions.c.epaspresdose == 0.0,
                    0.6 * CtePrescriptions.c.epaspresweight / 1000,
                ),
                (
                    CtePrescriptions.c.epaspresmixamount == 0.0,
                    CtePrescriptions.c.epaspresdose / 100,
                ),
                else_=CtePrescriptions.c.epaspresdose
                / CtePrescriptions.c.epaspresmixamount,
            ),
        ),
        "solumdr": func.coalesce(
            CteAdministrations.c.value0, CteAdministrations.c.value
        ),
        "vancomycin1g": (
            CteAdministrations.c.value
            * case(
                (CtePrescriptions.c.epaspresdose == 0, 1000),
                else_=CtePrescriptions.c.epaspresdose,
            )
            / case(
                (CtePrescriptions.c.epaspresmixamount == 0, 100),
                else_=CtePrescriptions.c.epaspresmixamount,
            )
        ),
        "privigeniv": CteAdministrations.c.value * 100,
    }

    return RECIPES.get(drug_name, None)


def get_quantity_recipe(
    CteAdministrations: Any = None,
    CtePrescriptions: Any = None,
    administration_type: str = None,
    drug_name: str = None,
) -> Any:
    """
    Takes CTEs of administrations and prescriptions plus an administration type
    and a drug name and returns the quantity recipe for administrations.
    The CTEs are usually subsets of the full administrations and prescriptions
    tables.
    """

    RECIPES: Final[Dict[str, Any]] = {
        "bolus": get_bolus_quantity_recipe(
            CteAdministrations, CtePrescriptions, drug_name
        ),
        "continuous": get_continuous_quantity_recipe(
            CteAdministrations, CtePrescriptions, drug_name
        ),
    }

    return RECIPES.get(administration_type, None)
