"""Recipes for calculating the quantity of drug administrations"""

from typing import Any, Dict, Final

from sqlalchemy import case, func, null


def get_bolus_recipes(
    CteAdministrations: Any = None,
    CtePrescriptions: Any = None,
) -> Any:
    """
    Takes CTEs of administrations and prescriptions plus an administration type
    and a drug name and returns the quantity recipe for administrations.
    The CTEs are usually subsets of the full administrations and prescriptions
    tables.
    """

    RECIPES: Final[Dict[str, Any]] = {
        "recipe__noradrenalinsad__bolus": func.coalesce(
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
        "recipe__solumdr__bolus": func.coalesce(
            CteAdministrations.c.value0, CteAdministrations.c.value
        ),
    }

    return RECIPES


def get_continuous_recipes(
    CteAdministrations: Any = None,
    CtePrescriptions: Any = None,
) -> Any:
    """
    Takes CTEs of administrations and prescriptions plus an administration type
    and a drug name and returns the quantity recipe for administrations.
    The CTEs are usually subsets of the full administrations and prescriptions
    tables.
    """

    RECIPES: Final[Dict[str, Any]] = {
        "recipe__metaoxedrinsad__continuous": CteAdministrations.c.value
        * func.coalesce(
            CteAdministrations.c.value / CteAdministrations.c.value1,
            CtePrescriptions.c.epaspresconc,
        ),
        "recipe__noradrenalinsad__continuous": func.coalesce(
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
        "recipe__solumdr__continuous": func.coalesce(
            CteAdministrations.c.value0, CteAdministrations.c.value
        ),
        "recipe__vancomycin1g__continuous": (
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
        "recipe__privigeniv__continuous": CteAdministrations.c.value * 100,
    }

    return RECIPES


def get_quantity_recipe(
    CteAdministrations: Any = None,
    CtePrescriptions: Any = None,
    administration_type: str = None,
    recipe_name: str = None,
    logger: Any = None,
) -> Any:
    """
    Takes CTEs of administrations and prescriptions plus an administration type
    and a drug name and returns the quantity recipe for administrations.
    The CTEs are usually subsets of the full administrations and prescriptions
    tables.
    """

    if administration_type == "bolus":
        RECIPES: Final[Dict[str, Any]] = get_bolus_recipes(
            CteAdministrations, CtePrescriptions
        )
    elif administration_type == "continuous":
        RECIPES: Final[Dict[str, Any]] = get_continuous_recipes(
            CteAdministrations, CtePrescriptions
        )
    else:
        raise NotImplementedError(
            "Recipes not implemented for administration type "
            + administration_type
        )

    if recipe_name is not None and recipe_name not in RECIPES:
        logger.warning(
            "  No conversion recipe found for %s (%s); quantities will be NULL.",
            recipe_name,
            administration_type,
        )

    return RECIPES.get(recipe_name, null())
