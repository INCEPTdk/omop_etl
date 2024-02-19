"""Source databse data models"""
# pylint: disable=too-many-lines
# pylint: disable=invalid-name
from typing import Any, Dict, Final, List

from sqlalchemy import Column

from ..util.freeze import freeze_instance
from .modelutils import (
    BigIntField,
    BoolField,
    CharField,
    FloatField,
    TimeStampField,
    make_model_base,
)

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


@register_source_model
@freeze_instance
class CourseMetadata(SourceModelBase):
    """
    The course_metadata source database
    """

    __tablename__: Final[str] = "course_metadata"
    __table_args__ = {"schema": SOURCE_SCHEMA}

    courseid: Final[Column] = BigIntField(nullable=False)
    timestamp: Final[Column] = TimeStampField(nullable=False)
    variable: Final[Column] = CharField(16, nullable=False)
    value: Final[Column] = FloatField(nullable=False)
    from_file: Final[Column] = CharField(7, nullable=False)


@register_source_model
@freeze_instance
class Administrations(SourceModelBase):
    """
    The administrations source database
    """

    __tablename__: Final[str] = "administrations"
    __table_args__ = {"schema": SOURCE_SCHEMA}

    courseid: Final[Column] = BigIntField(nullable=False)
    timestamp: Final[Column] = TimeStampField(nullable=False)
    epaspresbaseid: Final[Column] = BigIntField(nullable=False)
    variable: Final[Column] = CharField(22, nullable=False)
    value: Final[Column] = FloatField(nullable=False)
    from_file: Final[Column] = CharField(10, nullable=False)


@register_source_model
@freeze_instance
class Prescriptions(SourceModelBase):
    """
    The prescriptions source database
    """

    __tablename__: Final[str] = "prescriptions"
    __table_args__ = {"schema": SOURCE_SCHEMA}

    courseid: Final[Column] = BigIntField(nullable=False)
    timestamp: Final[Column] = TimeStampField(nullable=False)
    epaspresid: Final[Column] = BigIntField(primary_key=True)
    epaspresbaseid: Final[Column] = BigIntField(nullable=False)
    epaspresstarttime: Final[Column] = TimeStampField(nullable=False)
    epaspresdose: Final[Column] = FloatField(nullable=False)
    epaspresdosemax: Final[Column] = FloatField(nullable=False)
    epaspresdosestart: Final[Column] = FloatField(nullable=False)
    epaspresdrugunit: Final[Column] = CharField(7, nullable=False)
    epaspresdrugunitact: Final[Column] = CharField(12, nullable=False)
    epaspresconc: Final[Column] = FloatField(nullable=False)
    epaspresfluids: Final[Column] = CharField(10, nullable=False)
    epaspresinfusionmax: Final[Column] = FloatField(nullable=False)
    epaspresmaxconc: Final[Column] = FloatField(nullable=False)
    epaspresmaxbag: Final[Column] = BigIntField(nullable=False)
    epasprescreatetime: Final[Column] = TimeStampField(nullable=False)
    epaspresdisolved: Final[Column] = CharField(9, nullable=False)
    epaspresmixammount: Final[Column] = FloatField(nullable=False)
    epasprespn: Final[Column] = CharField(3, nullable=False)
    epaspresinint: Final[Column] = CharField(3, nullable=False)
    epaspresfreq: Final[Column] = CharField(30, nullable=False)
    epasprescreattype: Final[Column] = CharField(20, nullable=False)
    epaspresgsubst: Final[Column] = CharField(3, nullable=False)
    epasprespsubst: Final[Column] = CharField(3, nullable=False)
    epaspresdosemaxdaily: Final[Column] = FloatField(nullable=False)
    epaspresdosemaxtotal: Final[Column] = BigIntField(nullable=False)
    epaspresscheduletype: Final[Column] = CharField(1, nullable=False)
    epaspresdosemaxdailyunit: Final[Column] = CharField(7, nullable=False)
    epaspresdosemaxtotalunit: Final[Column] = CharField(1, nullable=False)
    epaspressecuritydose: Final[Column] = BigIntField(nullable=False)
    epaspressecuritydoseunit: Final[Column] = CharField(7, nullable=False)
    epaspressecuritydoseminutes: Final[Column] = BigIntField(nullable=False)
    epaspresminadmtime: Final[Column] = BigIntField(nullable=False)
    epaspresprotname: Final[Column] = CharField(43, nullable=False)
    epaspresprotname_right: Final[Column] = CharField(43, nullable=False)
    epaspresprotkey: Final[Column] = CharField(37, nullable=False)
    epaspresdrugname: Final[Column] = CharField(70, nullable=False)
    epaspresadmmthd: Final[Column] = CharField(117, nullable=False)
    epaspresdrugatc: Final[Column] = CharField(7, nullable=False)
    epaspresindication: Final[Column] = CharField(100, nullable=False)
    epaspresindictext: Final[Column] = CharField(100, nullable=False)
    epaspresindicsks: Final[Column] = CharField(100, nullable=False)
    epaspresdisctime: Final[Column] = TimeStampField(nullable=False)
    epaspresdiscreason: Final[Column] = CharField(36, nullable=False)
    epaspresadmroute: Final[Column] = CharField(2, nullable=False)
    epaspresgestage: Final[Column] = BigIntField(nullable=False)
    epaspresweight: Final[Column] = BigIntField(nullable=False)
    epaspresage: Final[Column] = BigIntField(nullable=False)
    epaspresbsa: Final[Column] = BigIntField(nullable=False)
    epasadmdoseunit: Final[Column] = CharField(4, nullable=False)
    epasadmdose: Final[Column] = FloatField(nullable=False)


@register_source_model
@freeze_instance
class DiagnosesProcedures(SourceModelBase):
    """
    The diagnoses_procedures source database
    """

    __tablename__: Final[str] = "diagnoses_procedures"
    __table_args__ = {"schema": SOURCE_SCHEMA}

    courseid: Final[Column] = BigIntField(nullable=False)
    timestamp: Final[Column] = CharField(100, nullable=False)
    variable: Final[Column] = CharField(16, nullable=False)
    value: Final[Column] = BoolField(nullable=False)
    from_file: Final[Column] = CharField(8, nullable=False)


@register_source_model
@freeze_instance
class Observations(SourceModelBase):
    """
    The observations- source database
    """

    __tablename__: Final[str] = "observations"
    __table_args__ = {"schema": SOURCE_SCHEMA}

    courseid: Final[Column] = BigIntField(nullable=False)
    timestamp: Final[Column] = TimeStampField(nullable=False)
    variable: Final[Column] = CharField(100, nullable=False)
    value: Final[Column] = CharField(580, nullable=False)
    from_file: Final[Column] = CharField(9, nullable=False)


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
