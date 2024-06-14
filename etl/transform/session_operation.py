"""The base transformation operation with a session"""

import logging
from typing import Any, Callable, Optional

from etl.models.omopcdm54.registry import OmopCdmModelBase
from etl.sql.merge.mergeutils import merge_cdm_table

from ..util.db import AbstractSession
from .base_operation import BaseOperation


class SessionOperation(BaseOperation):
    """The base transformation operation with session. Use this transformation if session is needed."""

    def __init__(
        self,
        key: str,
        session: AbstractSession,
        func: Callable[[AbstractSession], Any],
        description: Optional[str] = "",
    ) -> None:
        super().__init__(
            key=key,
            description=description,
        )
        self._func = func
        self.session = session

    def _run(self, *args, **kwargs) -> Any:
        return self._func(self.session)


class SessionOperationDefaultMerge(BaseOperation):
    """This operation is used exclusively for merging tables. it uses a default merge function"""

    def __init__(
        self,
        cdm_table: OmopCdmModelBase,
        session: AbstractSession,
        description: Optional[str] = "",
    ) -> None:
        super().__init__(
            key=str(cdm_table.__tablename__),
            description=description,
        )
        self.session = session
        self.cdm_table = cdm_table
        self.logger = logging.getLogger(f"ETL.Merge.{cdm_table.__name__}")

    def _run(self, *args, **kwargs) -> Any:
        self.logger.info("Starting the %s merge transformation... ", self.key)
        merge_cdm_table(self.session, self.cdm_table)
        self.logger.info(
            f"Merge {self.key} transformation complete. %s Row(s) included.",
            self.session.query(self.cdm_table).count(),
        )
