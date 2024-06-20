"""Create the tables needed for the ETL"""

import logging

from ..sql.create_omopcdm_tables import SQL, get_models_in_scope
from ..util.db import AbstractSession
from .transformutils import execute_sql_transform

logger = logging.getLogger("ETL.Core")


def transform(session: AbstractSession) -> None:
    """Create the OMOP CDM tables"""
    models_in_scope = get_models_in_scope()
    logger.info("Creating OMOP CDM tables in DB...")
    for m in models_in_scope:
        logger.debug("Creating table step %s: %s", m.__step__, m.__tablename__)
    execute_sql_transform(session, SQL)
    logger.info("OMOP CDM tables created successfully!")
