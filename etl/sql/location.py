""" Location transformation logic """

import os
from typing import Any, Final

from sqlalchemy import insert
from sqlalchemy.sql import Insert
from sqlalchemy.sql.functions import concat

from etl.csv import SHAK_LOOKUP
from etl.models.omopcdm54.health_systems import Location

DENMARK_CONCEPT_ID: Final[int] = 4330435

DEPARTMENT_SHAK_CODE = str(os.getenv("DEPARTMENT_SHAK_CODE"))


def get_postal_code(shak_lookup: dict[dict], department_shak_code: str) -> Any:
    return shak_lookup.get(department_shak_code, {}).get("postal_code")


POSTAL_CODE: Final[Any] = get_postal_code(SHAK_LOOKUP, DEPARTMENT_SHAK_CODE)


def get_location_insert(department_shak_code: str) -> Insert:
    return insert(Location).values(
        zip=get_postal_code(SHAK_LOOKUP, department_shak_code),
        location_source_value=concat(
            "department_shak_code|", department_shak_code
        ),
        country_concept_id=DENMARK_CONCEPT_ID,
    )
