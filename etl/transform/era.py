"""DrugEra and ConditionEra transformations"""
import logging

from ..util.db import AbstractSession
from .transformutils import execute_sql_file

logger = logging.getLogger("ETL.Era")


def transform(session: AbstractSession) -> None:
    """Create the Era tables"""
    logger.info("Creating Era tables in DB... ")
    execute_sql_file(session, "era.sql")
    logger.info("Era tables created successfully!")
