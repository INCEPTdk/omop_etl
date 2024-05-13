"""Care site transformations"""

import logging

from ..sql import DEPARTMENT_SHAK_CODE
from ..sql.care_site import CARE_SITE_COUNT, get_care_site_insert
from ..util.db import AbstractSession

logger = logging.getLogger("ETL.CareSite")


def transform(session: AbstractSession) -> None:
    """Run the care site transformation"""
    logger.info("Starting the Care site transformation... ")
    care_site_insert = get_care_site_insert(DEPARTMENT_SHAK_CODE)
    session.execute(care_site_insert)
    logger.info(
        "Care site transformation completed., %s Care site(s) included.",
        session.query(CARE_SITE_COUNT).scalar(),
    )
