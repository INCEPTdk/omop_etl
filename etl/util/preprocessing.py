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
                """\tConcept id %s is missing in the concept table of OMOP CDM database. It has been set to 0.""",
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


def _validate_domain_id(
    concept_id: int, domain_id: str, session: AbstractSession
) -> str:
    with session_context(session):
        valid_domain = (
            session.query(Concept.domain_id)
            .where(Concept.concept_id == concept_id)
            .scalar()
        )
        if valid_domain != domain_id and concept_id != 0:
            logger.debug(
                """Domain id %s is not valid for concept id %s. It has been set to %s.""",
                domain_id,
                concept_id,
                valid_domain,
            )
            return valid_domain
    return domain_id


def validate_domain_ids(
    input_df: pd.DataFrame,
    session: AbstractSession,
    concept_column: str,
    domain_column: str,
) -> pd.DataFrame:

    input_df[domain_column] = input_df.apply(
        lambda x: _validate_domain_id(
            x[concept_column], x[domain_column], session
        ),
        axis=1,
    ).astype(str)
    return input_df


def validate_timezones(
    input_df: pd.DataFrame, session: AbstractSession, timezone_column: str
) -> pd.DataFrame:
    original_tz = input_df[timezone_column]

    allowed_tz = set(session.scalars("SELECT name FROM pg_timezone_names()"))
    provided_tz = set(tz for tz in original_tz if not pd.isna(tz))

    for invalid_tz in provided_tz - allowed_tz:
        logger.debug(
            """Time zone %s invalid. It has been set to NULL.""", invalid_tz
        )

    new_tz = [str(tz) if tz in allowed_tz else None for tz in original_tz]
    input_df[timezone_column] = new_tz
    return input_df
