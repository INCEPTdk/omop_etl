"""Create the lookup tables needed for the ETL"""
import logging

from ..sql.create_lookup_tables import SQL
from ..util.db import AbstractSession
from .transformutils import execute_sql_transform

logger = logging.getLogger("ETL.Core")


def transform(session: AbstractSession) -> None:
    """Create the lookup tables"""
    logger.info("Creating lookup tables in DB... ")
    execute_sql_transform(session, SQL)
    logger.info("Lookup tables created successfully!")
