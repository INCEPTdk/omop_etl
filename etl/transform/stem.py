"""Stem transformations"""

import logging

from ..models.omopcdm54.clinical import Stem as OmopStem
from ..models.source import CourseMetadata, DiagnosesProcedures, Observations
from ..sql.stem import get_nondrug_stem_insert
from ..util.db import AbstractSession

logger = logging.getLogger("ETL.Stem")

NONDRUG_MODELS = [CourseMetadata, DiagnosesProcedures, Observations]


def transform(session: AbstractSession) -> None:
    """Run the Stem transformation"""
    logger.info("Starting the Stem transformation... ")
    for model in NONDRUG_MODELS:
        logger.info(
            "Transforming %s source data to the STEM table...",
            model.__tablename__,
        )
        session.execute(get_nondrug_stem_insert(session, model))
        logger.info(
            "STEM Transform in Progress, %s Events Included from source %s.",
            session.query(OmopStem)
            .where(OmopStem.datasource == model.__tablename__)
            .count(),
            model.__tablename__,
        )
    logger.info(
        "STEM Transformation complete! %s rows included",
        session.query(OmopStem).count(),
    )
