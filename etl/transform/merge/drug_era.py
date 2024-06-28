""" Merge drug era period tables """

import logging

from etl.sql.merge.mergeutils import (
    concatenate_overlapping_intervals,
    merge_cdm_table,
)

from ...models.omopcdm54.standardized_derived_elements import DrugEra
from ...util.db import AbstractSession

logger = logging.getLogger("ETL.Merge.DrugEra")


def concatenate_intervals(session: AbstractSession):

    SQL: str = concatenate_overlapping_intervals(
        DrugEra,
        key_columns=[
            DrugEra.person_id.key,
            DrugEra.drug_concept_id.key,
        ],
        start_date_column=DrugEra.drug_era_start_date.key,
        end_date_column=DrugEra.drug_era_end_date.key,
        agg_sum_columns=[DrugEra.drug_exposure_count.key, DrugEra.gap_days.key],
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

    concatenate_intervals(session)

    logger.info(
        "Merge Drug Era concatenate overlapping periods. Transformation complete! %s Era(s) included",
        session.query(DrugEra).count(),
    )
