"""Drug exposure transformations"""

import logging

from ..models.omopcdm54.clinical import DrugExposure as OmopDrugExposure
from ..sql.drug_exposure import DrugExposureInsert
from ..util.db import AbstractSession

logger = logging.getLogger("ETL.DrugExposure")


def transform(session: AbstractSession) -> None:
    """Run the Drug exposure transformation"""
    logger.info("Starting the drug exposure transformation... ")
    session.execute(DrugExposureInsert)
    logger.info(
        "Drug exposure Transformation complete! %s rows included",
        session.query(OmopDrugExposure).count(),
    )
