"""Merge Person transformations"""

import logging

from ...models.omopcdm54.clinical import Person
from ...sql.merge.mergeutils import drop_duplicate_rows, merge_cdm_table
from ...util.db import AbstractSession

logger = logging.getLogger("ETL.Merge.Person")


def transform(session: AbstractSession) -> None:
    """Run the Merge Person transformation"""
    logger.info("Starting the Person transformation... ")

    merge_cdm_table(session, Person)
    logger.info(
        "Merge Person Transformation with duplicates! %s Person(s) included",
        session.query(Person).count(),
    )
    session.execute(
        drop_duplicate_rows(
            Person, Person.person_source_value.key, Person.person_id.key
        )
    )
    logger.info(
        "Merge Person Transformation complete! %s Person(s) included",
        session.query(Person).count(),
    )
