""" Merge condition era period tables """

import logging

from etl.sql.merge.mergeutils import (
    concatenate_overlapping_intervals,
    merge_cdm_table,
)

from ...models.omopcdm54.standardized_derived_elements import ConditionEra
from ...util.db import AbstractSession

logger = logging.getLogger("ETL.Merge.DrugEra")


def concatenate_intervals(session: AbstractSession):

    SQL: str = concatenate_overlapping_intervals(
        ConditionEra,
        key_columns=[
            ConditionEra.person_id.key,
            ConditionEra.condition_concept_id.key,
        ],
        start_date_column=ConditionEra.condition_era_start_date.key,
        end_date_column=ConditionEra.condition_era_end_date.key,
        agg_sum_columns=ConditionEra.condition_occurrence_count.key,
    )
    session.execute(SQL)


def transform(session: AbstractSession) -> None:
    """Run the Merge Condition era transformation"""
    logger.info("Starting the Condition Era merge transformation... ")

    merge_cdm_table(session, ConditionEra)

    logger.info(
        "Merge Condition Era Transformation. Initial %s Era(s) included ...",
        session.query(ConditionEra).count(),
    )

    concatenate_intervals(session)

    logger.info(
        "Merge Condition Era concatenate overlapping periods. Transformation complete! %s Era(s) included",
        session.query(ConditionEra).count(),
    )
