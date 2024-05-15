"""Source databse data models"""

# pylint: disable=too-many-lines
# pylint: disable=invalid-name
from typing import Any, Dict, Final, List

from sqlalchemy import Column

from ..util.db import get_schema_name
from ..util.freeze import freeze_instance
from .modelutils import (
    BigIntField,
    BoolField,
    CharField,
    DateField,
    FloatField,
    PKIdMixin,
    PKIntField,
    TimeStampField,
    make_model_base,
)

SOURCE_SCHEMA: Final[str] = get_schema_name("SOURCE_SCHEMA", "source")
REGISTRY_SCHEMA: Final[str] = get_schema_name("REGISTRY_SCHEMA", "registries")

SourceModelBase: Any = make_model_base(schema=SOURCE_SCHEMA)
RegistryModelBase: Any = make_model_base(schema=REGISTRY_SCHEMA)


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
class CourseMetadata(SourceModelBase, PKIdMixin):
    """
    The course_metadata source table
    """

    __tablename__: Final[str] = "course_metadata"
    __table_args__ = {"schema": SOURCE_SCHEMA}

    courseid: Final[Column] = BigIntField()
    timestamp: Final[Column] = TimeStampField()
    variable: Final[Column] = CharField(50)
    value: Final[Column] = CharField(145)
    from_file: Final[Column] = CharField(50)


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
    variable: Final[Column] = CharField(70)
    value: Final[Column] = FloatField()
    from_file: Final[Column] = CharField(50)


@register_source_model
@freeze_instance
class Prescriptions(SourceModelBase, PKIdMixin):
    """
    The prescriptions source table
    """

    __tablename__: Final[str] = "prescriptions"
    __table_args__ = {"schema": SOURCE_SCHEMA}

    courseid: Final[Column] = BigIntField(nullable=False)
    timestamp: Final[Column] = TimeStampField()
    epaspresid: Final[Column] = BigIntField()
    epaspresbaseid: Final[Column] = BigIntField()
    epaspresstarttime: Final[Column] = TimeStampField()
    epaspresdose: Final[Column] = FloatField()
    epaspresdosemax: Final[Column] = FloatField()
    epaspresdosestart: Final[Column] = FloatField()
    epaspresdrugunit: Final[Column] = CharField(
        50,
    )
    epaspresdrugunitact: Final[Column] = CharField(
        50,
    )
    epaspresconc: Final[Column] = FloatField()
    epaspresfluids: Final[Column] = CharField(
        50,
    )
    epaspresinfusionmax: Final[Column] = FloatField()
    epaspresmaxconc: Final[Column] = FloatField()
    epaspresmaxbag: Final[Column] = BigIntField()
    epasprescreatetime: Final[Column] = TimeStampField()
    epaspresdisolved: Final[Column] = CharField(
        50,
    )
    epaspresmixammount: Final[Column] = FloatField()
    epasprespn: Final[Column] = CharField(
        50,
    )
    epaspresinint: Final[Column] = CharField(
        50,
    )
    epaspresfreq: Final[Column] = CharField(
        50,
    )
    epasprescreattype: Final[Column] = CharField(
        50,
    )
    epaspresgsubst: Final[Column] = CharField(
        50,
    )
    epasprespsubst: Final[Column] = CharField(
        50,
    )
    epaspresdosemaxdaily: Final[Column] = FloatField()
    epaspresdosemaxtotal: Final[Column] = BigIntField()
    epaspresscheduletype: Final[Column] = CharField(
        50,
    )
    epaspresdosemaxdailyunit: Final[Column] = CharField(
        50,
    )
    epaspresdosemaxtotalunit: Final[Column] = CharField(
        50,
    )
    epaspressecuritydose: Final[Column] = BigIntField()
    epaspressecuritydoseunit: Final[Column] = CharField(
        50,
    )
    epaspressecuritydoseminutes: Final[Column] = BigIntField()
    epaspresminadmtime: Final[Column] = BigIntField()
    epaspresprotname: Final[Column] = CharField(
        70,
    )
    epaspresprotname_right: Final[Column] = CharField(
        70,
    )
    epaspresprotkey: Final[Column] = CharField(
        70,
    )
    epaspresdrugname: Final[Column] = CharField(
        140,
    )
    epaspresadmmthd: Final[Column] = CharField(
        140,
    )
    epaspresdrugatc: Final[Column] = CharField(
        50,
    )
    epaspresindication: Final[Column] = CharField(
        140,
    )
    epaspresindictext: Final[Column] = CharField(
        140,
    )
    epaspresindicsks: Final[Column] = CharField(
        140,
    )
    epaspresdisctime: Final[Column] = TimeStampField()
    epaspresdiscreason: Final[Column] = CharField(
        70,
    )
    epaspresadmroute: Final[Column] = CharField(
        50,
    )
    epaspresgestage: Final[Column] = BigIntField()
    epaspresweight: Final[Column] = BigIntField()
    epaspresage: Final[Column] = BigIntField()
    epaspresbsa: Final[Column] = BigIntField()
    epasadmdoseunit: Final[Column] = CharField(
        50,
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
    timestamp: Final[Column] = TimeStampField()
    variable: Final[Column] = CharField(50)
    value: Final[Column] = BoolField()
    from_file: Final[Column] = CharField(50)


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
    variable: Final[Column] = CharField(140)
    value: Final[Column] = CharField(580)
    from_file: Final[Column] = CharField(50)


@register_source_model
@freeze_instance
class Person(RegistryModelBase, PKIdMixin):
    """
    The person source table
    """

    __tablename__: Final[str] = "person"
    __table_args__ = {"schema": REGISTRY_SCHEMA}

    cpr_enc: Final[Column] = CharField(50)
    c_kon: Final[Column] = CharField(50)
    d_foddato: Final[Column] = DateField()
    c_status: Final[Column] = CharField(50)
    d_status_hen_start: Final[Column] = DateField()


@register_source_model
@freeze_instance
class CourseIdCprMapping(SourceModelBase, PKIdMixin):
    """
    The courseid_cpr_mapping table
    """

    __tablename__: Final[str] = "courseid_cpr_mapping"
    __table_args__ = {"schema": SOURCE_SCHEMA}

    cpr_enc: Final[Column] = CharField(50, nullable=False)
    courseid: Final[Column] = BigIntField(nullable=False)


@register_source_model
@freeze_instance
class LabkaBccLaboratory(RegistryModelBase, PKIdMixin):
    """
    The table for lab data from LABKA and BCC
    """

    __tablename__: Final[str] = "laboratory"
    __table_args__ = {"schema": REGISTRY_SCHEMA}

    cpr_enc: Final[Column] = CharField(50, nullable=False)
    lab_id: Final[Column] = CharField(75)
    timestamp: Final[Column] = TimeStampField()
    component_simple_lookup: Final[Column] = CharField(255)
    clean_quantity_id: Final[Column] = CharField(50)
    unit_clean: Final[Column] = CharField(50)
    system_clean: Final[Column] = CharField(50)
    shown_clean: Final[Column] = CharField(50)
    ref_lower_clean: Final[Column] = CharField(50)
    ref_upper_clean: Final[Column] = CharField(50)
    interval_type: Final[Column] = CharField(50)
    flag: Final[Column] = CharField(50)
    abo: Final[Column] = CharField(50)
    rhesus: Final[Column] = CharField(50)


@register_source_model
@freeze_instance
class LprOperations(RegistryModelBase, PKIdMixin):
    """
    The operations table, adapted version of t_sksopr from NPR
    """

    __tablename__: Final[str] = "operations"
    __table_args__ = {"schema": REGISTRY_SCHEMA}

    cpr_enc: Final[Column] = CharField(50, nullable=False)
    start_date: Final[Column] = DateField()
    end_date: Final[Column] = DateField()
    sks_code: Final[Column] = CharField(50)
    sks_source: Final[Column] = CharField(50)


@register_source_model
@freeze_instance
class LprProcedures(RegistryModelBase, PKIdMixin):
    """
    The procedures table, adapted version of t_sksube from NPR
    """

    __tablename__: Final[str] = "procedures"
    __table_args__ = {"schema": REGISTRY_SCHEMA}

    cpr_enc: Final[Column] = CharField(50, nullable=False)
    start_date: Final[Column] = DateField()
    end_date: Final[Column] = DateField()
    sks_code: Final[Column] = CharField(50)
    sks_source: Final[Column] = CharField(50)


@register_source_model
@freeze_instance
class LprDiagnoses(RegistryModelBase, PKIdMixin):
    """
    The diagnoses table, adapted version of t_diag from NPR
    """

    __tablename__: Final[str] = "diagnoses"
    __table_args__ = {"schema": REGISTRY_SCHEMA}

    cpr_enc: Final[Column] = CharField(50, nullable=False)
    start_date: Final[Column] = DateField()
    end_date: Final[Column] = DateField()
    sks_code: Final[Column] = CharField(50)
    sks_source: Final[Column] = CharField(50)


SOURCE_VERSION: Final[str] = "0.1"

# pylint: disable=no-member
SOURCE_REGISTRY: Final[
    Dict[str, SourceModelBase]
] = SourceModelRegistry().registered  # type: ignore

# pylint: disable=no-member
SOURCE_MODELS: Final[
    List[SourceModelBase]
] = SourceModelRegistry().registered.values()  # type: ignore

# pylint: disable=no-member
SOURCE_MODEL_NAMES: Final[List[str]] = [
    k for k, _ in SourceModelRegistry().registered.items()
]

__all__ = SOURCE_MODEL_NAMES
