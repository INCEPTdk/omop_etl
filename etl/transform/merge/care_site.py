"""Merge CareSite transformations"""

import logging

from ...models.omopcdm54.health_systems import CareSite
from ...sql.merge.care_site import add_location_to_care_site
from ...sql.merge.mergeutils import merge_cdm_table
from ...util.db import AbstractSession

logger = logging.getLogger("ETL.Merge.CareSite")


def transform(session: AbstractSession) -> None:
    """Run the Merge location transformation"""
    logger.info("Starting the location transformation... ")

    merge_cdm_table(session, CareSite)
    session.execute(add_location_to_care_site())
    logger.info(
        "Merge Care Site Transformation complete! %s CareSite(s) included",
        session.query(CareSite).count(),
    )
