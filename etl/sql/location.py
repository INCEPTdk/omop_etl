""" Location transformation logic """

import os
from typing import Any, Final

import pandas as pd
from sqlalchemy import insert
from sqlalchemy.sql import Insert
from sqlalchemy.sql.functions import concat

from etl.csv import LOOKUP_DF
from etl.models.omopcdm54.health_systems import Location

DENMARK_CONCEPT_ID: Final[int] = 4330435

HOSPITAL_SHAK_CODE = os.getenv("HOSPITAL_SHAK_CODE")


def get_postal_code(shak_lookup: pd.DataFrame, shak_code: int):
    try:
        return str(
            shak_lookup.loc[
                shak_lookup["hospital_shak_code"] == int(shak_code),
                "postal_code",
            ].values[0]
        )
    except (IndexError, ValueError, TypeError):
        return None


POSTAL_CODE: Final[Any] = get_postal_code(LOOKUP_DF, HOSPITAL_SHAK_CODE)


def get_location_insert(shak_code: str) -> Insert:
    return insert(Location).values(
        zip=get_postal_code(LOOKUP_DF, shak_code),
        location_source_value=concat("hospital_shak_code|", shak_code),
        country_concept_id=DENMARK_CONCEPT_ID,
    )
