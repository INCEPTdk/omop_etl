"""Helper function for converting drug quantities to a standard units"""

from typing import Any, Dict, Final

from sqlalchemy import and_, case, null


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
        "recipe__corotropsnf": 0.001,
        "recipe__kaliumkloridps": 75,
        "recipe__kaliumkloridsad": 75,
        "recipe__minirinfrr": 1000,
        "recipe__novorapidiu": 1,
        "recipe__desmopressintv": 1,
    }

    if recipe_name is not None and recipe_name not in RECIPES:
        logger.warning(
            "  No conversion recipe found for %s; quantities will be NULL.",
            recipe_name,
        )
        return null()

    return RECIPES.get(recipe_name, 1.0)
