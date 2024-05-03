"""Device exposure transformations"""

import logging

from ..models.omopcdm54.clinical import DeviceExposure as OmopDeviceExposure
from ..sql.device_exposure import DeviceExposureInsert
from ..util.db import AbstractSession

logger = logging.getLogger("ETL.DeviceExposure")


def transform(session: AbstractSession) -> None:
    """Run the device exposure transformation"""
    logger.info("Starting the device exposure transformation... ")
    session.execute(DeviceExposureInsert)
    logger.info(
        "Device exposure Transformation complete! %s rows included",
        session.query(OmopDeviceExposure).count(),
    )
