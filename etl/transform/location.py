"""Location transformations"""
import logging

from ..models.omopcdm54.health_systems import Location
from ..sql import DEPARTMENT_SHAK_CODE
from ..sql.location import POSTAL_CODE, get_location_insert
from ..util.db import AbstractSession

logger = logging.getLogger("ETL.Location")


def transform(session: AbstractSession) -> None:
    """Run the location transformation"""
    logger.info("Starting the location transformation... ")
    session.execute(get_location_insert(DEPARTMENT_SHAK_CODE))
    logger.info(
        "LOCATION Transformation complete! %s Location(s) included",
        session.query(Location).count(),
    )
    if not POSTAL_CODE:
        logger.warning(
            "Could not find shak_code %s in the lookup file, this code needs to be added manually",
            DEPARTMENT_SHAK_CODE,
        )
