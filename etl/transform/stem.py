"""Stem transformations"""

import logging
import os

from sqlalchemy import and_

from ..models.omopcdm54.clinical import Stem as OmopStem
from ..models.source import (
    Administrations,
    CourseMetadata,
    DiagnosesProcedures,
    LabkaBccLaboratory,
    LprDiagnoses,
    LprOperations,
    LprProcedures,
    Observations,
)
from ..sql.stem import (
    get_drug_stem_insert,
    get_laboratory_stem_insert,
    get_mapped_nondrug_stem_insert,
    get_registry_stem_insert,
    get_unmapped_nondrug_stem_insert,
)
from ..sql.stem.utils import (
    get_batches_from_concept_loopkup_stem,
    validate_source_variables,
)
from ..util.db import AbstractSession, get_environment_variable

logger = logging.getLogger("ETL.Stem")

NONDRUG_MODELS = [CourseMetadata, DiagnosesProcedures, Observations]
DRUG_MODELS = [Administrations]
REGISTRY_MODELS = [LprDiagnoses, LprProcedures, LprOperations]
LABORATORY_MODELS = [LabkaBccLaboratory]
BATCH_SIZE = int(get_environment_variable("BATCH_SIZE", "5"))


def transform(session: AbstractSession) -> None:
    """Run the Stem transformation"""
    logger.info("Starting the Stem transformation... ")

    for model in (
        NONDRUG_MODELS + DRUG_MODELS + REGISTRY_MODELS + LABORATORY_MODELS
    ):
        validate_source_variables(session, model, logger)

    transform_non_drug_models(session)
    transform_drug_models(session)
    transform_registry_models(session)
    transform_laboratory_models(session)

    count_rows = session.query(OmopStem).count()
    n_mapped_rows = (
        session.query(OmopStem).where(OmopStem.concept_id.isnot(None)).count()
    )

    logger.info(
        "STEM Transformation complete! %s rows included, of which %s were mapped to a concept_id (%s%%).",
        count_rows,
        n_mapped_rows,
        round(n_mapped_rows / max(1, count_rows) * 100, 2),
    )


def transform_non_drug_models(session: AbstractSession) -> None:

    for model in NONDRUG_MODELS:
        logger.info(
            "%s source data to the STEM table...",
            model.__tablename__.upper(),
        )

        for ConceptLookupStemBatchCte in get_batches_from_concept_loopkup_stem(
            model, session, batch_size=BATCH_SIZE, logger=logger
        ):
            session.execute(
                get_mapped_nondrug_stem_insert(
                    session, model, ConceptLookupStemBatchCte
                )
            )
            session.commit()

        logger.info(
            "STEM Transform in Progress, %s Events Included from mapped nondrug source %s.",
            session.query(OmopStem)
            .where(OmopStem.datasource == model.__tablename__)
            .count(),
            model.__tablename__,
        )

        if os.getenv("INCLUDE_UNMAPPED_CODES", "TRUE") == "TRUE":
            session.execute(get_unmapped_nondrug_stem_insert(session, model))

            logger.info(
                "STEM Transform in Progress, %s Events including unmapped nondrug source %s.",
                session.query(OmopStem)
                .where(OmopStem.datasource == model.__tablename__)
                .count(),
                model.__tablename__,
            )


def transform_drug_models(session: AbstractSession) -> None:
    logger.info("DRUG source data to the STEM table...")
    session.execute(get_drug_stem_insert(session, logger))

    logger.info(
        "STEM Transform in Progress, %s Events Included from source administrations.",
        session.query(OmopStem)
        .where(OmopStem.datasource.like("%_administrations"))
        .count(),
    )

    drug_records_in_stem = (
        session.query(OmopStem)
        .where(
            and_(OmopStem.domain_id == "Drug", OmopStem.concept_id.isnot(None))
        )
        .count()
    )

    drug_records_with_quantity = (
        session.query(OmopStem)
        .where(
            and_(
                OmopStem.domain_id == "Drug",
                OmopStem.concept_id.isnot(None),
                OmopStem.quantity_or_value_as_number.isnot(None),
            )
        )
        .count()
    )

    logger.info(
        "STEM Transform in Progress, %s Drug Events Included, of which %s (%s%%) have a quantity.",
        drug_records_in_stem,
        drug_records_with_quantity,
        round(
            drug_records_with_quantity / max(1, drug_records_in_stem) * 100, 2
        ),
    )


def transform_registry_models(session: AbstractSession) -> None:
    for model in REGISTRY_MODELS:
        logger.info(
            "%s source data to the STEM table...",
            model.__tablename__.upper(),
        )
        session.execute(get_registry_stem_insert(session, model))
        logger.info(
            "STEM Transform in Progress, %s Events Included from source %s.",
            session.query(OmopStem)
            .where(OmopStem.datasource == model.__tablename__)
            .count(),
            model.__tablename__,
        )


def transform_laboratory_models(session: AbstractSession) -> None:
    for model in LABORATORY_MODELS:
        logger.info(
            "%s source data to the STEM table...",
            model.__tablename__.upper(),
        )
        session.execute(get_laboratory_stem_insert(session, model))
        logger.info(
            "STEM Transform in Progress, %s Events Included from source %s.",
            session.query(OmopStem)
            .where(OmopStem.datasource == model.__tablename__)
            .count(),
            model.__tablename__,
        )
