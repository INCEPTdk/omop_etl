"""Stem transformations"""

import logging

from sqlalchemy import and_

from ..models.omopcdm54.clinical import Stem as OmopStem
from ..models.source import (
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
    get_nondrug_stem_insert,
    get_registry_stem_insert,
)
from ..util.db import AbstractSession

logger = logging.getLogger("ETL.Stem")

NONDRUG_MODELS = [CourseMetadata, DiagnosesProcedures, Observations]
REGISTRY_MODELS = [LprDiagnoses, LprProcedures, LprOperations]
LABORATORY_MODELS = [LabkaBccLaboratory]


def transform(session: AbstractSession) -> None:
    """Run the Stem transformation"""
    logger.info("Starting the Stem transformation... ")

    for model in NONDRUG_MODELS:
        logger.info(
            "%s source data to the STEM table...",
            model.__tablename__.upper(),
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

    logger.info("DRUG source data to the STEM table...")
    session.execute(get_drug_stem_insert(session))

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
                OmopStem.quantity.isnot(None),
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

    count_rows = session.query(OmopStem).count()
    mapped_rows = (
        session.query(OmopStem).where(OmopStem.concept_id.isnot(None)).count()
    )

    logger.info(
        "STEM Transformation complete! %s rows included, of which %s were mapped to a concept_id (%s%%).",
        count_rows,
        mapped_rows,
        round(mapped_rows / max(1, count_rows) * 100, 2),
    )


x = "fa;jfkl;ajfla;f"
x.split(
    ";",
)
