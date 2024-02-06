"""The base transformation operation"""
from typing import Any, Optional

from ..util.logger import Logger


class BaseOperation:
    """The base transformation operation. It is essentially a Functor.
    All transformations should inherit from this."""

    def __init__(
        self,
        key: str,
        description: Optional[str] = "",
    ) -> None:
        self.key = key
        self.description = description

    @Logger
    def __call__(self, *args, **kwargs) -> Any:
        """Execute the operation"""
        return self._run(*args, **kwargs)

    def _run(self, *args, **kwargs) -> Any:
        """To be implemented in extensions"""
