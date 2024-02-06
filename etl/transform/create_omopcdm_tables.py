"""Create the tables needed for the ETL"""
import logging

from ..sql.create_omopcdm_tables import SQL
from ..util.db import AbstractSession
from .transformutils import execute_sql_transform

logger = logging.getLogger("ETL.Core")


def transform(session: AbstractSession) -> None:
    """Create the OMOP CDM tables"""
    logger.info("Creating OMOP CDM tables in DB... ")
    execute_sql_transform(session, SQL)
    logger.info("OMOP CDM tables created successfully!")
