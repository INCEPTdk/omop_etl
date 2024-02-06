""" batch transform helpers """
from typing import Any, Generator

import pandas as pd

from .db import DataBaseWriterBuilder, Session, WriteMode


class DataFrameBatch:
    """
    A simple object representing a batch item for dataframes to the target database.
    """

    def __init__(self, target_session: Session, table_name: str, model: Any):
        self._session = target_session
        self.table_name = table_name
        self.model = model
        self._insert_count = 0
        self.columns = [c.key for c in self.model.__table__.columns.values()]

        self._writer = (
            DataBaseWriterBuilder()
            .set_header(False)
            .set_delimiter("|")
            .set_write_mode(WriteMode.APPEND)
            .build()
        )

    def insert(self, fetched: Any) -> int:
        """Create a dataframe from the fetched data and insert it into the target database"""
        batch_df = pd.DataFrame(fetched, columns=self.columns)
        self._insert_count = 0
        if not batch_df.empty:
            self._writer.set_source(self.model, batch_df).write(
                self._session, columns=self.columns
            )
            self._insert_count = len(batch_df)
        return self._insert_count

    @property
    def nr_inserted(self) -> int:
        """Get the number of rows inserted last"""
        return self._insert_count


def source_to_target_batch(
    source_cursor: Any, target_batch: DataFrameBatch, limit: int
) -> Generator[int, None, None]:
    """A generator performing the fetch-create-insert"""
    count = -1
    while count != 0:
        fetched = source_cursor.fetchmany(limit)
        count = target_batch.insert(fetched)
        yield count


def batch_process(
    source_cursor: Any,
    target_batch: DataFrameBatch,
    limit: int,
) -> int:
    """
    Batch process the source data to target
    """
    row_count = sum(source_to_target_batch(source_cursor, target_batch, limit))
    return row_count
