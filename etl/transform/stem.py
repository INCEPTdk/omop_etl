"""Stem transformations"""

import logging

from ..models.omopcdm54.clinical import Stem as OmopStem
from ..models.source import (
    CourseMetadata,
    DiagnosesProcedures,
    LprDiagnoses,
    LprOperations,
    LprProcedures,
    Observations,
)
from ..sql.stem import get_nondrug_stem_insert, get_registry_stem_insert
from ..util.db import AbstractSession

logger = logging.getLogger("ETL.Stem")

NONDRUG_MODELS = [CourseMetadata, DiagnosesProcedures, Observations]
REGISTRY_MODELS = [LprDiagnoses, LprProcedures, LprOperations]


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

    for model in REGISTRY_MODELS:
        logger.info(
            "Transforming %s source data to the STEM table...",
            model.__tablename__,
        )
        session.execute(get_registry_stem_insert(model))
        logger.info(
            "STEM Transform in Progress, %s Events Included from source %s.",
            session.query(OmopStem)
            .where(OmopStem.datasource == model.__tablename__)
            .count(),
            model.__tablename__,
        )

    count_rows = session.query(OmopStem).count()
    mapped_rows = (
        session.query(OmopStem).where(OmopStem.concept_id.isnot(None)).count()
    )

    logger.info(
        "STEM Transformation complete! %s rows included, %s rows where mapped to a concept_id (%s%%).",
        count_rows,
        mapped_rows,
        round(mapped_rows / count_rows * 100, 2),
    )
