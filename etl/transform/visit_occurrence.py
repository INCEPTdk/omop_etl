"""Visit occurrence transformations"""
import logging

from etl.models.omopcdm54.clinical import VisitOccurrence

from ..sql import DEPARTMENT_SHAK_CODE
from ..sql.visit_occurrence import (
    get_count_courseid_dates_not_matching,
    get_count_courseid_missing_dates,
    get_visit_occurrence_insert,
)
from ..util.db import AbstractSession

logger = logging.getLogger("ETL.VisitOccurrence")


def transform(session: AbstractSession) -> None:
    """Run the visit occurrence transformation"""
    logger.info("Starting the visit occurrence transformation... ")

    session.execute(get_visit_occurrence_insert(DEPARTMENT_SHAK_CODE))
    logger.info(
        "Visit occurrence Transformation complete! %s rows included",
        session.query(VisitOccurrence).count(),
    )
    date_not_found = get_count_courseid_missing_dates(DEPARTMENT_SHAK_CODE)
    date_mismatch = get_count_courseid_dates_not_matching(DEPARTMENT_SHAK_CODE)

    count_date_not_found = session.query(date_not_found).count()
    count_date_mismatch = session.query(date_mismatch).count()

    logger.info(
        "Visit Occurrence: %d rows excluded because missing date.",
        count_date_not_found,
    )
    logger.info(
        "Visit Occurrence: %d rows excluded because date mismatch.",
        count_date_mismatch,
    )
