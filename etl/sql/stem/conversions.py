"""Helper function for converting drug quantities to a standard units"""

from fractions import Fraction
from typing import Any, Dict, Final

from sqlalchemy import and_, case, null


def is_float(value):
    try:
        Fraction(value)
        return True
    except ValueError:
        return False


def get_conversion_factor(
    Administrations: Any = None,
    Prescriptions: Any = None,
    recipe_name: str = None,
    logger: Any = None,
) -> Any:

    RECIPES: Final[Dict[str, Any]] = {
        "recipe__noradrenalinsad": case(
            (Prescriptions.epaspresdrugunit == "ug", 0.001),
            else_=1,
        ),
        "recipe__g_to_mg": case(
            (Prescriptions.epaspresdrugunit == "g", 1000),
            else_=1,
        ),
        "recipe__vancomycin1g": case(
            (
                and_(
                    Prescriptions.epaspresdrugunit == "g",
                    Prescriptions.epaspresdose == 0.0,
                    Administrations.administration_type == "discrete",
                ),
                1000,
            ),
            else_=1,
        ),
        "recipe__metaoxedrinsad": case(
            (Prescriptions.epaspresdrugunit == "ug", 0.001),
            (
                Prescriptions.epaspresdrugunit == "ml",
                Prescriptions.epaspresconc,
            ),
            else_=1,
        ),
    }

    if recipe_name is not None and recipe_name not in RECIPES:

        if is_float(recipe_name):
            conversion_factor = Fraction(recipe_name)
        else:
            logger.warning(
                "  Recipe %s is not recognized or is invalid multiplier for quantity; quantities will be NULL.",
                recipe_name,
            )
            conversion_factor = null()
    else:
        conversion_factor = RECIPES.get(recipe_name, 1.0)

    return conversion_factor
