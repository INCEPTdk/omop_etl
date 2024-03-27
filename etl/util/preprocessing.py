"""
Preprocessing the source data
"""
import logging

import pandas as pd
from sqlalchemy import and_

from etl.models.omopcdm54.vocabulary import Concept
from etl.util.db import AbstractSession, session_context

logger = logging.getLogger("ETL.Core.PreProcessing")


def _validate_concept_id(concept_id: int, session: AbstractSession) -> int:
    if not concept_id:
        return 0
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
    ).astype(int)
    return input_df
