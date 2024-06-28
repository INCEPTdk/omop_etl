""" Merge condition era period tables """

import logging

from etl.sql.merge.condition_era import concatenate_overlapping_intervals
from etl.sql.merge.mergeutils import merge_cdm_table

from ...models.omopcdm54.standardized_derived_elements import ConditionEra
from ...util.db import AbstractSession

logger = logging.getLogger("ETL.Merge.DrugEra")


def transform(session: AbstractSession) -> None:
    """Run the Merge Condition era transformation"""
    logger.info("Starting the Condition Era merge transformation... ")

    merge_cdm_table(session, ConditionEra)

    logger.info(
        "Merge Condition Era Transformation. Initial %s Era(s) included ...",
        session.query(ConditionEra).count(),
    )

    SQL: str = concatenate_overlapping_intervals()
    session.execute(SQL)

    logger.info(
        "Merge Condition Era concatenate overlapping periods. Transformation complete! %s Era(s) included",
        session.query(ConditionEra).count(),
    )
