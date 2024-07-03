"""Recipes for calculating the quantity of drug administrations"""

from typing import Any, Dict, Final

from sqlalchemy import case, func, null


def get_bolus_recipes(
    Administrations: Any = None,
    Prescriptions: Any = None,
) -> Any:
    """
    Takes CTEs of administrations and prescriptions plus an administration type
    and a drug name and returns the quantity recipe for administrations.
    The CTEs are usually subsets of the full administrations and prescriptions
    tables.
    """

    RECIPES: Final[Dict[str, Any]] = {
        "recipe__noradrenalinsad__bolus": func.coalesce(
            Administrations.value0,
            Administrations.value
            * case(
                (  # weight-based dose
                    Prescriptions.epaspresdose == 0.0,
                    0.6 * Prescriptions.epaspresweight / 1000,
                ),
                (  # assume disolved in 100 mL
                    Prescriptions.epaspresmixamount == 0.0,
                    Prescriptions.epaspresdose / 100,
                ),
                else_=Prescriptions.epaspresdose
                / Prescriptions.epaspresmixamount,
            ),
        ),
        "recipe__solumdr__bolus": func.coalesce(
            Administrations.value0, Administrations.value
        ),
    }

    return RECIPES


def get_continuous_recipes(
    Administrations: Any = None,
    Prescriptions: Any = None,
) -> Any:
    """
    Takes CTEs of administrations and prescriptions plus an administration type
    and a drug name and returns the quantity recipe for administrations.
    The CTEs are usually subsets of the full administrations and prescriptions
    tables.
    """

    RECIPES: Final[Dict[str, Any]] = {
        "recipe__metaoxedrinsad__continuous": Administrations.value
        * func.coalesce(
            Administrations.value / Administrations.value1,
            Prescriptions.epaspresconc,
        ),
        "recipe__noradrenalinsad__continuous": func.coalesce(
            Administrations.value0,
            Administrations.value
            * case(
                (
                    Prescriptions.epaspresdose == 0.0,
                    0.6 * Prescriptions.epaspresweight / 1000,
                ),
                (
                    Prescriptions.epaspresmixamount == 0.0,
                    Prescriptions.epaspresdose / 100,
                ),
                else_=Prescriptions.epaspresdose
                / Prescriptions.epaspresmixamount,
            ),
        ),
        "recipe__solumdr__continuous": func.coalesce(
            Administrations.value0, Administrations.value
        ),
        "recipe__vancomycin1g__continuous": (
            Administrations.value
            * case(
                (Prescriptions.epaspresdose == 0, 1000),
                else_=Prescriptions.epaspresdose,
            )
            / case(
                (Prescriptions.epaspresmixamount == 0, 100),
                else_=Prescriptions.epaspresmixamount,
            )
        ),
        "recipe__privigeniv__continuous": Administrations.value * 100,
    }

    return RECIPES


def get_quantity_recipe(
    Administrations: Any = None,
    Prescriptions: Any = None,
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
            Administrations, Prescriptions
        )
    elif administration_type == "continuous":
        RECIPES: Final[Dict[str, Any]] = get_continuous_recipes(
            Administrations, Prescriptions
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
