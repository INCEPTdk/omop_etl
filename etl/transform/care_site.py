"""Care site transformations"""
import logging

from ..sql.care_site import get_care_site_insert
from ..sql import DEPARTMENT_SHAK_CODE
from ..util.db import AbstractSession

logger = logging.getLogger("ETL.CareSite")


def transform(session: AbstractSession) -> None:
    """Run the care site transformation"""
    logger.info("Starting the Care site transformation... ")
    care_site_insert = get_care_site_insert(DEPARTMENT_SHAK_CODE)
    session.execute(care_site_insert)
    logger.info(
        "Care site transformation completed.",
    )
