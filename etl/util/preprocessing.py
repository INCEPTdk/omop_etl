"""
Preprocessing the source data
"""
import logging
from typing import Dict

import numpy as np
import pandas as pd
from sqlalchemy import and_

from etl.models.omopcdm54.vocabulary import Concept
from etl.util.db import AbstractSession, session_context

logger = logging.getLogger("ETL.Core.PreProcessing")


def all_object_columns_lower_case(input_df: pd.DataFrame) -> pd.DataFrame:
    for column in input_df.select_dtypes(
        include=object, exclude=["datetime", "timedelta"]
    ):
        try:
            input_df[column] = input_df[column].str.lower()
        # skip non confirming object fields
        except AttributeError:
            continue
    return input_df


def replace_to_nan(input_df: pd.DataFrame) -> pd.DataFrame:
    for column in input_df:
        input_df[column].replace(
            ["nan", "none", "<not performed>"], np.nan, inplace=True
        )
    return input_df


def set_columns_to_lowercase(input_df: pd.DataFrame) -> pd.DataFrame:
    input_df.columns = map(str.lower, input_df.columns)
    return input_df


# pylint: disable=unused-argument
def log_missing_columns(tablename: str, input_df: pd.DataFrame) -> None:
    # TO-DO: implement
    pass


def preprocessing_transform(
    data: Dict[str, pd.DataFrame]
) -> Dict[str, pd.DataFrame]:
    for key, value in data.items():
        data[key] = all_object_columns_lower_case(value)
        data[key] = replace_to_nan(value)
        data[key] = set_columns_to_lowercase(value)
        log_missing_columns(key, value)
    return data


def _validate_concept_id(concept_id: int, session: AbstractSession) -> int:
    with session_context(session):
        counter_result = (
            session.query(Concept)
            .where(
                and_(
                    Concept.concept_id == concept_id,
                    Concept.standard_concept == "S",
                )
            )
            .count()
        )
        if counter_result == 0:
            logger.debug(
                """Concept id %s is missing in the concept table of OMOP CDM database. It has been set to 0.""",
                concept_id,
            )
            return 0
    return concept_id


def validate_concept_ids(
    input_df: pd.DataFrame, session: AbstractSession, concept_column: str
) -> pd.DataFrame:
    # Validates concept ids. If they are not present in the existing concept ids, it will log
    # the concept_id and set it to 0.

    input_df[concept_column] = input_df.apply(
        lambda x: _validate_concept_id(x[concept_column], session),
        axis=1,
    )
    return input_df
