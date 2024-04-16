"""Procedure occurrence transformations"""

import logging

from ..models.omopcdm54.clinical import (
    ProcedureOccurrence as OmopProcedureOccurrence,
)
from ..sql.procedure_occurrence import ProcedureOccurrenceInsert
from ..util.db import AbstractSession

logger = logging.getLogger("ETL.ProcedureOccurrence")


def transform(session: AbstractSession) -> None:
    """Run the Procedure occurrence transformation"""
    logger.info("Starting the Procedure occurrence transformation... ")
    session.execute(ProcedureOccurrenceInsert)
    logger.info(
        "Procedure occurrence Transformation complete! %s rows included",
        session.query(OmopProcedureOccurrence).count(),
    )
