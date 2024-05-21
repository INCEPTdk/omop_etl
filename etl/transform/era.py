"""DrugEra and ConditionEra transformations"""

import logging

from ..sql.eras.drug_era import DrugEraInsert
from ..util.db import AbstractSession

logger = logging.getLogger("ETL.Era")


def transform(session: AbstractSession) -> None:
    """Create the Era tables"""
    logger.info("Creating Era tables in DB... ")
    logger.info("  Creating DRUG ERA table...")
    session.execute(DrugEraInsert)

    # here will the other era queries be executed
    logger.info("Era tables created successfully!")
