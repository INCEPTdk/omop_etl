"""Transform specific utils"""

import logging
import os

from ..util.db import AbstractSession

logger = logging.getLogger("ETL.Core")


def execute_sql_transform(session: AbstractSession, sql: str) -> None:
    """Execute sql for a given session"""
    session.execute(sql)


def execute_sql_file(
    session: AbstractSession, filename: str, encoding="utf-8"
) -> None:
    """Execute SQL given a filename containing the SQL statements"""
    parent_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")
    with open(f"{parent_dir}/sql/{filename}", "r", encoding=encoding) as fsql:
        session.execute(fsql.read())
