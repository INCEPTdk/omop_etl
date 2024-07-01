""" Merge condition era period tables """

import logging

from etl.sql.merge.mergeutils import _unite_intervals_sql, merge_cdm_table

from ...models.omopcdm54.standardized_derived_elements import ConditionEra
from ...util.db import AbstractSession

logger = logging.getLogger("ETL.Merge.DrugEra")


def unite_intervals(session: AbstractSession):

    SQL: str = _unite_intervals_sql(
        ConditionEra,
        key_columns=[
            ConditionEra.person_id.key,
            ConditionEra.condition_concept_id.key,
        ],
        interval_start_column=ConditionEra.condition_era_start_date.key,
        interval_end_column=ConditionEra.condition_era_end_date.key,
        agg_columns=ConditionEra.condition_occurrence_count.key,
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

    unite_intervals(session)

    logger.info(
        "Merge Condition Era unite overlapping periods. Transformation complete! %s Era(s) included",
        session.query(ConditionEra).count(),
    )
