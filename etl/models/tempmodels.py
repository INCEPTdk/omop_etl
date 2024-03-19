"""Temporary table data models"""
# pylint: disable=too-many-lines
# pylint: disable=invalid-name
from typing import Any, Dict, Final, List

from sqlalchemy import Column

from ..util.freeze import freeze_instance
from .modelutils import FK, CharField, IntField, NumericField, make_model_base
from .omopcdm54 import Concept

LOOKUPS_SCHEMA: Final[str] = "lookups"
TempModelBase: Any = make_model_base(schema=LOOKUPS_SCHEMA)


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
    __table_args__ = {"schema": LOOKUPS_SCHEMA}

    lookup_id: Final[Column] = IntField(primary_key=True)
    concept_string: Final[Column] = CharField(200)
    concept_id: Final[Column] = IntField(FK(Concept.concept_id))
    filter: Final[Column] = CharField(50)


@register_temp_model
@freeze_instance
class ConceptLookupStem(TempModelBase):
    """lookup table to map source concepts to target concept_ids"""

    __tablename__: Final = "concept_lookup_stem"
    __table_args__ = {"schema": LOOKUPS_SCHEMA}

    uid: Final[Column] = IntField(primary_key=True)
    datasource: Final[Column] = CharField(200)
    source_file: Final[Column] = NumericField()
    source_concept_code: Final[Column] = CharField(50)
    source_variable: Final[Column] = CharField(50)
    value: Final[Column] = CharField(50)
    value_type: Final[Column] = CharField(50)
    variable_definition: Final[Column] = CharField(200)
    value_definition: Final[Column] = CharField(200)
    start_date: Final[Column] = CharField(50)
    end_date: Final[Column] = CharField(50)
    provider: Final[Column] = CharField(50)
    care_site: Final[Column] = CharField(50)
    type_concept_id: Final[Column] = IntField()
    mapped_standard_code: Final[Column] = IntField()
    mapped_std_code_desc: Final[Column] = CharField(200)
    std_code_vocabulary: Final[Column] = CharField(30)
    std_code_concept_code: Final[Column] = CharField(200)
    std_code_domain: Final[Column] = CharField(30)
    reviewer_comment: Final[Column] = CharField(200)
    value_as_number: Final[Column] = CharField(50)
    value_as_concept_id: Final[Column] = IntField()
    value_as_string: Final[Column] = CharField(200)
    operator_concept_id: Final[Column] = IntField()
    unit_source_value: Final[Column] = CharField(50)
    unit_concept_id: Final[Column] = IntField()
    conversion: Final[Column] = NumericField()
    modifier_concept_id: Final[Column] = IntField()
    anatomic_site_concept_id: Final[Column] = IntField()
    quantity: Final[Column] = CharField(50)
    days_supply: Final[Column] = CharField(50)
    dose_unit_source_value: Final[Column] = CharField(50)
    range_low: Final[Column] = NumericField()
    range_high: Final[Column] = NumericField()
    stop_reason: Final[Column] = CharField(20)
    route_concept_id: Final[Column] = IntField()
    route_source_value: Final[Column] = CharField(50)
    refills: Final[Column] = CharField(50)


TEMP_VERSION: Final[str] = "0.1"

# pylint: disable=no-member
SOURCE_REGISTRY: Final[
    Dict[str, TempModelBase]  # type: ignore
] = TempModelRegistry().registered

# pylint: disable=no-member
TEMP_MODELS: Final[
    List[TempModelBase]  # type: ignore
] = TempModelRegistry().registered.values()

# pylint: disable=no-member
TEMP_MODEL_NAMES: Final[List[str]] = [
    k for k, _ in TempModelRegistry().registered.items()
]

__all__ = TEMP_MODEL_NAMES
