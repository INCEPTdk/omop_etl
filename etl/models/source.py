"""Source databse data models"""
# pylint: disable=too-many-lines
# pylint: disable=invalid-name
from typing import Any, Dict, Final, List

# from ..util.freeze import freeze_instance
from .modelutils import make_model_base

# from sqlalchemy import Column


SOURCE_SCHEMA: Final[str] = "source"

SourceModelBase: Any = make_model_base(schema=SOURCE_SCHEMA)


class SourceModelRegistry:
    """A simple global registry for source tables"""

    __shared_state = {"registered": {}}

    def __init__(self) -> None:
        self.__dict__ = self.__shared_state


def register_source_model(cls: Any) -> Any:
    """Class decorator to add model to registry"""
    borg = SourceModelRegistry()
    # pylint: disable=no-member
    borg.registered[cls.__name__] = cls
    return cls


SOURCE_VERSION: Final[str] = "0.1"

# pylint: disable=no-member
SOURCE_REGISTRY: Final[
    Dict[str, SourceModelBase]
] = SourceModelRegistry().registered

# pylint: disable=no-member
SOURCE_MODELS: Final[
    List[SourceModelBase]
] = SourceModelRegistry().registered.values()

# pylint: disable=no-member
SOURCE_MODEL_NAMES: Final[List[str]] = [
    k for k, _ in SourceModelRegistry().registered.items()
]

__all__ = SOURCE_MODEL_NAMES
