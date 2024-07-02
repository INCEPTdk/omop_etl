""" Merge drug era period tables """

import logging

from etl.sql.merge.mergeutils import _unite_intervals_sql, merge_cdm_table

from ...models.omopcdm54.standardized_derived_elements import DrugEra
from ...util.db import AbstractSession

logger = logging.getLogger("ETL.Merge.DrugEra")


def unite_intervals(session: AbstractSession):

    SQL: str = _unite_intervals_sql(
        DrugEra,
        key_columns=[
            DrugEra.person_id.key,
            DrugEra.drug_concept_id.key,
        ],
        interval_start_column=DrugEra.drug_era_start_date.key,
        interval_end_column=DrugEra.drug_era_end_date.key,
        agg_columns=[DrugEra.drug_exposure_count.key, DrugEra.gap_days.key],
    )

    session.execute(SQL)


def transform(session: AbstractSession) -> None:
    """Run the Merge Drug era transformation"""
    logger.info("Starting the Drug Era merge transformation... ")

    merge_cdm_table(session, DrugEra)

    logger.info(
        "Merge Drug Era Transformation. Initial %s Era(s) included ...",
        session.query(DrugEra).count(),
    )

    unite_intervals(session)

    logger.info(
        "Merge Drug Era unite overlapping periods. Transformation complete! %s Era(s) included",
        session.query(DrugEra).count(),
    )
