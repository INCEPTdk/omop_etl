"""Drug era transformation"""

import logging

from ..models.omopcdm54.standardized_derived_elements import (
    DrugEra as OmopDrugEra,
)
from ..sql.drug_era import get_drug_era_insert
from ..util.db import AbstractSession

logger = logging.getLogger("ETL.DrugEra")


def transform(session: AbstractSession) -> None:
    """Run the Drug era transformation"""
    logger.info("Starting the drug era transformation... ")
    session.execute(get_drug_era_insert(session))
    logger.info(
        "Drug era Transformation complete! %s rows included",
        session.query(OmopDrugEra).count(),
    )
