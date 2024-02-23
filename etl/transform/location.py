"""Location transformations"""
import logging

from ..models.omopcdm54.health_systems import Location
from ..sql.location import HOSPITAL_SHAK_CODE, LOCATION_INSERT, POSTAL_CODE
from ..util.db import AbstractSession

logger = logging.getLogger("ETL.Location")


def transform(session: AbstractSession) -> None:
    """Run the location transformation"""
    logger.info("Starting the location transformation... ")
    session.execute(LOCATION_INSERT)
    logger.info(
        "Location transformation finished successfully with count %s",
        session.query(Location).count(),
    )
    if not POSTAL_CODE:
        logger.warning(
            "Could not find shak_code %s in the lookup file, this code needs to be added manually",
            HOSPITAL_SHAK_CODE,
        )
