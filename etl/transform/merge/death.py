""" Merge Death tables """

import logging

from etl.sql.merge.mergeutils import drop_duplicate_rows, merge_cdm_table

from ...models.omopcdm54.clinical import Death
from ...util.db import AbstractSession

logger = logging.getLogger("ETL.Merge.Death")


def transform(session: AbstractSession) -> None:
    """Run the Merge location transformation"""
    logger.info("Starting the Death merge transformation... ")

    merge_cdm_table(session, Death)

    logger.info(
        "Merge Death Transformation. Initial %s Death(s) included ...",
        session.query(Death).count(),
    )

    # pylint: disable=no-member
    session.execute(
        drop_duplicate_rows(Death, Death.person_id.key, Death.death_id.key)
    )

    logger.info(
        "Merge Death Removed duplicates. Transformation complete! %s Death(s) included",
        session.query(Death).count(),
    )
