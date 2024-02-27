""" Care site transformation logic """

from typing import Any, Final, Dict

import pandas as pd
from sqlalchemy import insert, literal, select
from sqlalchemy.sql import Insert
from sqlalchemy.sql.functions import concat

from etl.csv.lookups import SHAK_LOOKUP_DF, get_concept_lookup_dict
from etl.models.omopcdm54.health_systems import CareSite, Location


def get_department_info(
    shak_lookup: pd.DataFrame, shak_code: str, col_info: str
) -> Any:
    try:
        return str(
            shak_lookup.loc[
                shak_lookup["department_shak_code"] == str(shak_code),
                col_info,
            ].values[0]
        )
    except (IndexError, ValueError, TypeError):
        return None


PLACE_OF_SERVICE_CONCEPT_ID_LOOKUP: Final[
    Dict[str, int]
] = get_concept_lookup_dict(CareSite.__tablename__)


def get_care_site_insert(shak_code: str) -> Insert:

    return insert(CareSite).from_select(
        names=[
            CareSite.location_id,
            CareSite.care_site_name,
            CareSite.place_of_service_concept_id,
            CareSite.care_site_source_value,
            CareSite.place_of_service_source_value,
        ],
        select=select(
            [
                Location.location_id,
                literal(
                    get_department_info(
                        SHAK_LOOKUP_DF, shak_code, "department_name"
                    )
                ),
                literal(
                    PLACE_OF_SERVICE_CONCEPT_ID_LOOKUP[
                        get_department_info(
                            SHAK_LOOKUP_DF, shak_code, "department_type"
                        )
                    ]
                ),
                concat("department_shak_code|", shak_code),
                concat(
                    "department_type|",
                    get_department_info(
                        SHAK_LOOKUP_DF, shak_code, "department_type"
                    ),
                ),
            ],
        )
        .select_from(Location)
        .where(
            Location.location_source_value
            == concat("department_shak_code|", shak_code)
        ),
    )
