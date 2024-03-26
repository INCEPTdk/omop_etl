"""Create the lookup tables needed for the ETL"""
import logging
from typing import Dict

import pandas as pd

from ..models.tempmodels import TEMP_MODELS
from ..sql.create_lookup_tables import SQL
from ..util.db import AbstractSession, df_to_sql
from .transformutils import execute_sql_transform

logger = logging.getLogger("ETL.Core.CreateLookupTables")


def transform(
    session: AbstractSession, lookup_loader: Dict[str, pd.DataFrame]
) -> None:
    """Create the lookup tables"""
    logger.info("Creating lookup tables in DB... ")
    execute_sql_transform(session, SQL)
    for model in TEMP_MODELS:
        temp_df = lookup_loader[model.__tablename__]
        df_to_sql(
            session=session,
            dataframe=temp_df,
            table=str(model.__table__),
            columns=temp_df.columns,
        )
    logger.info("Lookup tables created successfully!")
