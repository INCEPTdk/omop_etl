"""Stem transformations"""

import logging

from ..models.omopcdm54.clinical import Stem as OmopStem
from ..models.source import (
    SOURCE_SCHEMA,
    Administrations,
    CourseMetadata,
    DiagnosesProcedures,
    Observations,
)
from ..sql.stem import SQL_FUNCTIONS
from ..util.db import AbstractSession

logger = logging.getLogger("ETL.Stem")

MODELS = [Administrations, CourseMetadata, DiagnosesProcedures, Observations]


def transform(session: AbstractSession) -> None:
    """Run the Stem transformation"""
    logger.info("Starting the Stem transformation... ")
    session.execute(SQL_FUNCTIONS)
    for model in MODELS:
        logger.info(
            "Transforming %s source data to the STEM table...",
            model.__tablename__,
        )
        session.execute(
            f"SELECT * FROM omopcdm.date_cols('{SOURCE_SCHEMA}','{model.__tablename__}');"
        )
        session.execute(
            f"SELECT COUNT(*) FROM omopcdm.stem WHERE datasource = '{model.__tablename__}'"
        )
        try:
            stem_overview = session.cursor().fetchall()
            logger.info(
                "STEM Transform in Progress, %s Events Included from source %s.",
                stem_overview[0][0],
                model.__tablename__,
            )
        except:  # noqa: E722
            logger.info(
                "No results to fetch from %s",
                model.__tablename__,
            )
    logger.info(
        "STEM Transformation complete! %s rows included",
        session.query(OmopStem).count(),
    )
