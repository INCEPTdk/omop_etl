"""Visit detail transformations"""
import logging

from ..sql import DEPARTMENT_SHAK_CODE
from ..sql.visit_occurrence import get_visit_occurrence_insert, CourseIdMapped
from ..util.db import AbstractSession

logger = logging.getLogger("ETL.VisitDetailTransformation")


def transform(session: AbstractSession) -> None:
    """Run the visit detail transformation"""
    logger.info("Starting the visit occurrence transformation... ")
    df = session.query(CourseIdMapped.subquery())
    import pdb; pdb.set_trace()
    session.execute(get_visit_occurrence_insert(DEPARTMENT_SHAK_CODE))
    logger.info(
        "Visit Occurrence Transformation complete!",
    )
