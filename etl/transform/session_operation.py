"""The base transformation operation with a session"""
from typing import Any, Callable, Optional

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