"""Meerge Location transformations"""

import logging

from ...models.omopcdm54.health_systems import Location
from ...sql.merge.location import get_location_merge_insert
from ...util.db import AbstractSession, get_source_cdm_schemas

logger = logging.getLogger("ETL.Merge.Location")


def transform(session: AbstractSession) -> None:
    """Run the Merge location transformation"""
    logger.info("Starting the location transformation... ")
    schemas = get_source_cdm_schemas(session)
    session.execute(get_location_merge_insert(schemas))
    logger.info(
        "MERGE LOCATION Transformation complete! %s Location(s) included",
        session.query(Location).count(),
    )
