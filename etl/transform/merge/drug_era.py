""" Merge drug era period tables """

import logging

from etl.sql.merge.drug_era import concatenate_overlapping_intervals
from etl.sql.merge.mergeutils import merge_cdm_table

from ...models.omopcdm54.standardized_derived_elements import DrugEra
from ...util.db import AbstractSession

logger = logging.getLogger("ETL.Merge.DrugEra")


def transform(session: AbstractSession) -> None:
    """Run the Merge Drug era transformation"""
    logger.info("Starting the Drug Era merge transformation... ")

    merge_cdm_table(session, DrugEra)

    logger.info(
        "Merge Drug Era Transformation. Initial %s Era(s) included ...",
        session.query(DrugEra).count(),
    )

    SQL: str = concatenate_overlapping_intervals()
    session.execute(SQL)

    logger.info(
        "Merge Drug Era concatenate overlapping periods. Transformation complete! %s Era(s) included",
        session.query(DrugEra).count(),
    )
