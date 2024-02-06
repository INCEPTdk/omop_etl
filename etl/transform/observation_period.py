"""ObservationPeriod transformations"""
import logging

from ..sql.observation_period import SQL
from ..util.db import AbstractSession
from .transformutils import execute_sql_transform

logger = logging.getLogger("ETL.ObservationPeriod")


def transform(session: AbstractSession) -> None:
    """Create the ObservationPeriod tables"""
    logger.info("Creating ObservationPeriod table in DB... ")
    execute_sql_transform(session, SQL)
    logger.info("ObservationPeriod tables created successfully!")
