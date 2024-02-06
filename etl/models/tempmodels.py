"""Temporary table data models"""
# pylint: disable=too-many-lines
# pylint: disable=invalid-name
from typing import Any, Dict, Final, List

from ..util.freeze import freeze_instance
from .modelutils import FK, CharField, IntField, make_model_base
from .omopcdm54 import Concept

TempModelBase: Any = make_model_base()


class TempModelRegistry:
    """A simple global registry for temporary tables"""

    __shared_state = {"registered": {}}

    def __init__(self) -> None:
        self.__dict__ = self.__shared_state


def register_temp_model(cls: Any) -> Any:
    """Class decorator to add model to registry"""
    borg = TempModelRegistry()
    # pylint: disable=no-member
    borg.registered[cls.__name__] = cls
    return cls


# TO-DO: Add models here for lookups, temp tables, etc


@register_temp_model
@freeze_instance
class ConceptLookup(TempModelBase):
    """lookup table to map source concepts to target concept_ids"""

    __tablename__: Final = "concept_lookup"
    __table_args__ = {"schema": "lookups"}

    lookup_id: Final = IntField(primary_key=True)
    concept_string: Final = CharField(200)
    concept_id: Final = IntField(FK(Concept.concept_id))
    filter: Final = CharField(50)


TEMP_VERSION: Final[str] = "0.1"

# pylint: disable=no-member
SOURCE_REGISTRY: Final[
    Dict[str, TempModelBase]
] = TempModelRegistry().registered

# pylint: disable=no-member
TEMP_MODELS: Final[
    List[TempModelBase]
] = TempModelRegistry().registered.values()

# pylint: disable=no-member
TEMP_MODEL_NAMES: Final[List[str]] = [
    k for k, _ in TempModelRegistry().registered.items()
]

__all__ = TEMP_MODEL_NAMES
