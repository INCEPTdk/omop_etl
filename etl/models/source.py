"""Source databse data models"""
# pylint: disable=too-many-lines
# pylint: disable=invalid-name
import os
from typing import Any, Dict, Final, List

from sqlalchemy import Column
from sqlalchemy.orm import declarative_mixin

from ..util.freeze import freeze_instance
from .modelutils import (
    BigIntField,
    BoolField,
    CharField,
    DateField,
    FloatField,
    TimeStampField,
    make_model_base,
)

SOURCE_SCHEMA: Final[str] = os.getenv("SOURCE_SCHEMA", default="source")

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


@declarative_mixin
class PKIdMixin:
    """A mixin"""

    _id = BigIntField(primary_key=True)


@register_source_model
@freeze_instance
class CourseMetadata(SourceModelBase, PKIdMixin):
    """
    The course_metadata source table
    """

    __tablename__: Final[str] = "course_metadata"
    __table_args__ = {"schema": SOURCE_SCHEMA}

    courseid: Final[Column] = BigIntField()
    timestamp: Final[Column] = TimeStampField()
    variable: Final[Column] = CharField(16)
    value: Final[Column] = FloatField()
    from_file: Final[Column] = CharField(7)


@register_source_model
@freeze_instance
class Administrations(SourceModelBase, PKIdMixin):
    """
    The administrations source table
    """

    __tablename__: Final[str] = "administrations"
    __table_args__ = {"schema": SOURCE_SCHEMA}

    courseid: Final[Column] = BigIntField(nullable=False)
    timestamp: Final[Column] = TimeStampField()
    epaspresbaseid: Final[Column] = BigIntField()
    variable: Final[Column] = CharField(22)
    value: Final[Column] = FloatField()
    from_file: Final[Column] = CharField(10)


@register_source_model
@freeze_instance
class Prescriptions(SourceModelBase):
    """
    The prescriptions source table
    """

    __tablename__: Final[str] = "prescriptions"
    __table_args__ = {"schema": SOURCE_SCHEMA}

    courseid: Final[Column] = BigIntField(nullable=False)
    timestamp: Final[Column] = TimeStampField()
    epaspresid: Final[Column] = BigIntField(primary_key=True)
    epaspresbaseid: Final[Column] = BigIntField()
    epaspresstarttime: Final[Column] = TimeStampField()
    epaspresdose: Final[Column] = FloatField()
    epaspresdosemax: Final[Column] = FloatField()
    epaspresdosestart: Final[Column] = FloatField()
    epaspresdrugunit: Final[Column] = CharField(
        7,
    )
    epaspresdrugunitact: Final[Column] = CharField(
        12,
    )
    epaspresconc: Final[Column] = FloatField()
    epaspresfluids: Final[Column] = CharField(
        10,
    )
    epaspresinfusionmax: Final[Column] = FloatField()
    epaspresmaxconc: Final[Column] = FloatField()
    epaspresmaxbag: Final[Column] = BigIntField()
    epasprescreatetime: Final[Column] = TimeStampField()
    epaspresdisolved: Final[Column] = CharField(
        9,
    )
    epaspresmixammount: Final[Column] = FloatField()
    epasprespn: Final[Column] = CharField(
        3,
    )
    epaspresinint: Final[Column] = CharField(
        3,
    )
    epaspresfreq: Final[Column] = CharField(
        30,
    )
    epasprescreattype: Final[Column] = CharField(
        20,
    )
    epaspresgsubst: Final[Column] = CharField(
        3,
    )
    epasprespsubst: Final[Column] = CharField(
        3,
    )
    epaspresdosemaxdaily: Final[Column] = FloatField()
    epaspresdosemaxtotal: Final[Column] = BigIntField()
    epaspresscheduletype: Final[Column] = CharField(
        1,
    )
    epaspresdosemaxdailyunit: Final[Column] = CharField(
        7,
    )
    epaspresdosemaxtotalunit: Final[Column] = CharField(
        1,
    )
    epaspressecuritydose: Final[Column] = BigIntField()
    epaspressecuritydoseunit: Final[Column] = CharField(
        7,
    )
    epaspressecuritydoseminutes: Final[Column] = BigIntField()
    epaspresminadmtime: Final[Column] = BigIntField()
    epaspresprotname: Final[Column] = CharField(
        43,
    )
    epaspresprotname_right: Final[Column] = CharField(
        43,
    )
    epaspresprotkey: Final[Column] = CharField(
        37,
    )
    epaspresdrugname: Final[Column] = CharField(
        70,
    )
    epaspresadmmthd: Final[Column] = CharField(
        117,
    )
    epaspresdrugatc: Final[Column] = CharField(
        7,
    )
    epaspresindication: Final[Column] = CharField(
        100,
    )
    epaspresindictext: Final[Column] = CharField(
        100,
    )
    epaspresindicsks: Final[Column] = CharField(
        100,
    )
    epaspresdisctime: Final[Column] = TimeStampField()
    epaspresdiscreason: Final[Column] = CharField(
        36,
    )
    epaspresadmroute: Final[Column] = CharField(
        2,
    )
    epaspresgestage: Final[Column] = BigIntField()
    epaspresweight: Final[Column] = BigIntField()
    epaspresage: Final[Column] = BigIntField()
    epaspresbsa: Final[Column] = BigIntField()
    epasadmdoseunit: Final[Column] = CharField(
        4,
    )
    epasadmdose: Final[Column] = FloatField()


@register_source_model
@freeze_instance
class DiagnosesProcedures(SourceModelBase, PKIdMixin):
    """
    The diagnoses_procedures source table
    """

    __tablename__: Final[str] = "diagnoses_procedures"
    __table_args__ = {"schema": SOURCE_SCHEMA}

    courseid: Final[Column] = BigIntField(nullable=False)
    timestamp: Final[Column] = CharField(100)
    variable: Final[Column] = CharField(16)
    value: Final[Column] = BoolField()
    from_file: Final[Column] = CharField(8)


@register_source_model
@freeze_instance
class Observations(SourceModelBase, PKIdMixin):
    """
    The observations- source table
    """

    __tablename__: Final[str] = "observations"
    __table_args__ = {"schema": SOURCE_SCHEMA}

    courseid: Final[Column] = BigIntField(nullable=False)
    timestamp: Final[Column] = TimeStampField()
    variable: Final[Column] = CharField(100)
    value: Final[Column] = CharField(580)
    from_file: Final[Column] = CharField(9)


@register_source_model
@freeze_instance
class Person(SourceModelBase, PKIdMixin):
    """
    The person source table
    """

    __tablename__: Final[str] = "person"
    __table_args__ = {"schema": SOURCE_SCHEMA}

    cpr_enc: Final[Column] = CharField(
        26,
    )
    c_kon: Final[Column] = CharField(
        1,
    )
    d_foddato: Final[Column] = DateField()
    c_status: Final[Column] = CharField(
        2,
    )
    d_status_hen_start: Final[Column] = DateField()


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
