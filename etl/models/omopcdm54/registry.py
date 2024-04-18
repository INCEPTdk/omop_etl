"""OMOP CDM model common utilities"""

import os
from typing import Any, Final

from ..modelutils import make_model_base

TARGET_SCHEMA: Final[str] = os.getenv("TARGET_SCHEMA", default="omopcdm")

OmopCdmModelBase: Any = make_model_base(schema=TARGET_SCHEMA)


class OmopCdmModelRegistry:
    """A simple global registry for analytical database tables"""

    __shared_state = {"registered": {}}

    def __init__(self) -> None:
        self.__dict__ = self.__shared_state


def register_omop_model(cls: Any) -> Any:
    """Class decorator to add model to registry"""
    borg = OmopCdmModelRegistry()
    # pylint: disable=no-member
    borg.registered[cls.__name__] = cls
    return cls
