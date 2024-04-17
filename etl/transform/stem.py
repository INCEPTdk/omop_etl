"""Stem transformations"""

import logging

from ..models.omopcdm54.clinical import Stem as OmopStem
from ..models.source import CourseMetadata, DiagnosesProcedures, Observations
from ..models.tempmodels import ConceptLookupStem
from ..sql.stem import create_simple_stem_insert
from ..util.db import AbstractSession
from ..util.misc import find_datetime_columns, flatten_to_set

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

        unique_datetime_column_names = flatten_to_set(
            (
                session.query(
                    ConceptLookupStem.start_date, ConceptLookupStem.end_date
                )
                .where(ConceptLookupStem.datasource == model.__tablename__)
                .distinct()
                .all()
            )
        )
        if not unique_datetime_column_names:
            # To keep in the stem table even without concept_lookup_stem matches
            unique_datetime_column_names = find_datetime_columns(model)

        unique_value_as_number_columns = flatten_to_set(
            (
                session.query(ConceptLookupStem.value_as_number)
                .where(ConceptLookupStem.datasource == model.__tablename__)
                .distinct()
                .all()
            )
        )
        if not unique_value_as_number_columns:
            # Need something to pop() below
            unique_value_as_number_columns = {None}

        unique_value_as_string_columns = flatten_to_set(
            (
                session.query(ConceptLookupStem.value_as_string)
                .where(ConceptLookupStem.datasource == model.__tablename__)
                .distinct()
                .all()
            )
        )
        if not unique_value_as_string_columns:
            # Need something to pop() below
            unique_value_as_string_columns = {None}

        if (
            len(unique_datetime_column_names) == 1
            and len(unique_value_as_number_columns) == 1
            and len(unique_value_as_string_columns) == 1
        ):
            InsertSql = create_simple_stem_insert(
                model,
                unique_datetime_column_names.pop(),
                unique_value_as_number_columns.pop(),
                unique_value_as_string_columns.pop(),
            )
        else:
            # this then needs to unpivot both value_as_number, value_as_string and datetimes columns
            # InsertSql = create_complex_stem_insert(...)
            pass

        session.execute(InsertSql)

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
