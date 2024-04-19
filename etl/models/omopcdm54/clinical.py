"""clinical models for OMOPCDM"""

# pylint: disable=invalid-name
from typing import Final

from sqlalchemy.orm import declarative_mixin, declared_attr

from ...util.freeze import freeze_instance
from ..modelutils import (
    FK,
    CharField,
    Column,
    DateField,
    DateTimeField,
    IntField,
    NumericField,
    PKIdMixin,
)
from .health_systems import CareSite, Location, Provider
from .registry import OmopCdmModelBase as ModelBase, register_omop_model
from .vocabulary import Concept


@register_omop_model
@freeze_instance
class Person(ModelBase):
    """
    https://ohdsi.github.io/CommonDataModel/cdm54.html#PERSON
    """

    __tablename__: Final[str] = "person"

    person_id: Final[Column] = IntField(primary_key=True)
    gender_concept_id: Final[Column] = IntField(
        FK(Concept.concept_id), nullable=False
    )
    year_of_birth: Final[Column] = IntField(nullable=False)
    month_of_birth: Final[Column] = IntField()
    day_of_birth: Final[Column] = IntField()
    birth_datetime: Final[Column] = DateTimeField()
    race_concept_id: Final[Column] = IntField(
        FK(Concept.concept_id), nullable=False
    )
    ethnicity_concept_id: Final[Column] = IntField(
        FK(Concept.concept_id), nullable=False
    )
    location_id: Final[Column] = IntField(FK(Location.location_id))
    provider_id: Final[Column] = IntField(FK(Provider.provider_id))
    care_site_id: Final[Column] = IntField(FK(CareSite.care_site_id))
    person_source_value: Final[Column] = CharField(50)
    gender_source_value: Final[Column] = CharField(50)
    gender_source_concept_id: Final[Column] = IntField(FK(Concept.concept_id))
    race_source_value: Final[Column] = CharField(50)
    race_source_concept_id: Final[Column] = IntField(FK(Concept.concept_id))
    ethnicity_source_value: Final[Column] = CharField(50)
    ethnicity_source_concept_id: Final[Column] = IntField(
        FK(Concept.concept_id)
    )


@declarative_mixin
class PersonIdMixin:
    """Mixin for person_id"""

    @declared_attr
    # pylint: disable=no-self-argument
    def person_id(cls) -> Column:
        return IntField(FK(Person.person_id), nullable=False)


@register_omop_model
@freeze_instance
class ObservationPeriod(ModelBase, PersonIdMixin):
    """
    https://ohdsi.github.io/CommonDataModel/cdm54.html#OBSERVATION_PERIOD
    """

    __tablename__: Final[str] = "observation_period"

    observation_period_id: Final[Column] = IntField(primary_key=True)
    observation_period_start_date: Final[Column] = DateField(nullable=False)
    observation_period_end_date: Final[Column] = DateField(nullable=False)
    period_type_concept_id: Final[Column] = IntField(
        FK(Concept.concept_id), nullable=False
    )


@declarative_mixin
class CareSiteIdMixin:
    """Mixin for care_site_id"""

    @declared_attr
    # pylint: disable=no-self-argument
    def care_site_id(cls) -> Column:
        return IntField(FK(CareSite.care_site_id))


@declarative_mixin
class ProviderIdMixin:
    """Mixin for provider_id"""

    @declared_attr
    # pylint: disable=no-self-argument
    def provider_id(cls) -> Column:
        return IntField(FK(Provider.provider_id))


@register_omop_model
@freeze_instance
class VisitOccurrence(
    ModelBase, PersonIdMixin, CareSiteIdMixin, ProviderIdMixin
):
    """https://ohdsi.github.io/CommonDataModel/cdm54.html#VISIT_OCCURRENCE"""

    __tablename__: Final[str] = "visit_occurrence"

    visit_occurrence_id: Final[Column] = IntField(primary_key=True)
    visit_concept_id: Final[Column] = IntField(
        FK(Concept.concept_id), nullable=False
    )
    visit_start_date: Final[Column] = DateField(nullable=False)
    visit_start_datetime: Final[Column] = DateTimeField()
    visit_end_date: Final[Column] = DateField(nullable=False)
    visit_end_datetime: Final[Column] = DateTimeField()
    visit_type_concept_id: Final[Column] = IntField(
        FK(Concept.concept_id), nullable=False
    )
    visit_source_value: Final[Column] = CharField(50)
    visit_source_concept_id: Final[Column] = IntField(FK(Concept.concept_id))
    admitted_from_concept_id: Final[Column] = IntField(FK(Concept.concept_id))
    admitted_from_source_value: Final[Column] = CharField(50)
    discharged_to_concept_id: Final[Column] = IntField(FK(Concept.concept_id))
    discharged_to_source_value: Final[Column] = CharField(50)
    preceding_visit_occurrence_id: Final[Column] = IntField(
        FK("visit_occurrence.visit_occurrence_id")
    )


class VisitOccurrenceIdMixin:
    """Mixin for visit_occurrence_id"""

    @declared_attr
    # pylint: disable=no-self-argument
    def visit_occurrence_id(cls) -> Column:
        return IntField(FK(VisitOccurrence.visit_occurrence_id))


@register_omop_model
@freeze_instance
class VisitDetail(
    ModelBase,
    PersonIdMixin,
    CareSiteIdMixin,
    ProviderIdMixin,
    VisitOccurrenceIdMixin,
):
    """https://ohdsi.github.io/CommonDataModel/cdm54.html#VISIT_DETAIL"""

    __tablename__: Final[str] = "visit_detail"

    visit_detail_id: Final[Column] = IntField(primary_key=True)
    visit_detail_concept_id: Final[Column] = IntField(
        FK(Concept.concept_id), nullable=False
    )
    visit_detail_start_date: Final[Column] = DateField(nullable=False)
    visit_detail_start_datetime: Final[Column] = DateTimeField()
    visit_detail_end_date: Final[Column] = DateField(nullable=False)
    visit_detail_end_datetime: Final[Column] = DateTimeField()
    visit_detail_type_concept_id: Final[Column] = IntField(
        FK(Concept.concept_id), nullable=False
    )
    visit_detail_source_value: Final[Column] = CharField(50)
    visit_detail_source_concept_id: Final[Column] = IntField(
        FK(Concept.concept_id)
    )
    admitted_from_concept_id: Final[Column] = IntField(FK(Concept.concept_id))
    admitted_from_source_value: Final[Column] = CharField(50)
    discharged_to_concept_id: Final[Column] = IntField(FK(Concept.concept_id))
    discharged_to_source_value: Final[Column] = CharField(50)
    preceding_visit_detail_id: Final[Column] = IntField(
        FK("visit_detail.visit_detail_id")
    )
    parent_visit_detail_id: Final[Column] = IntField(
        FK("visit_detail.visit_detail_id")
    )


@declarative_mixin
class VisitAndProviderMixin(VisitOccurrenceIdMixin, ProviderIdMixin):
    """Mixin for provider_id, visit_occurrence_id, and visit_detail_id"""

    @declared_attr
    # pylint: disable=no-self-argument
    def visit_detail_id(cls) -> Column:
        return IntField(FK(VisitDetail.visit_detail_id))


@register_omop_model
@freeze_instance
class ConditionOccurrence(ModelBase, PersonIdMixin, VisitAndProviderMixin):
    """https://ohdsi.github.io/CommonDataModel/cdm54.html#CONDITION_OCCURRENCE"""

    __tablename__: Final[str] = "condition_occurrence"

    condition_occurrence_id: Final[Column] = IntField(primary_key=True)
    condition_concept_id: Final[Column] = IntField(
        FK(Concept.concept_id), nullable=False
    )
    condition_start_date: Final[Column] = DateField(nullable=False)
    condition_start_datetime: Final[Column] = DateTimeField()
    condition_end_date: Final[Column] = DateField()
    condition_end_datetime: Final[Column] = DateTimeField()
    condition_type_concept_id: Final[Column] = IntField(
        FK(Concept.concept_id), nullable=False
    )
    condition_status_concept_id: Final[Column] = IntField(
        FK(Concept.concept_id)
    )
    stop_reason: Final[Column] = CharField(20)
    condition_source_value: Final[Column] = CharField(50)
    condition_source_concept_id: Final[Column] = IntField(
        FK(Concept.concept_id)
    )
    condition_status_source_value: Final[Column] = CharField(50)


@register_omop_model
@freeze_instance
class DrugExposure(ModelBase, PersonIdMixin, VisitAndProviderMixin):
    """https://ohdsi.github.io/CommonDataModel/cdm54.html#DRUG_EXPOSURE"""

    __tablename__: Final[str] = "drug_exposure"

    drug_exposure_id: Final[Column] = IntField(primary_key=True)
    drug_concept_id: Final[Column] = IntField(
        FK(Concept.concept_id), nullable=False
    )
    drug_exposure_start_date: Final[Column] = DateField(nullable=False)
    drug_exposure_start_datetime: Final[Column] = DateTimeField()
    drug_exposure_end_date: Final[Column] = DateField(nullable=False)
    drug_exposure_end_datetime: Final[Column] = DateTimeField()
    verbatim_end_date: Final[Column] = DateField()
    drug_type_concept_id: Final[Column] = IntField(
        FK(Concept.concept_id), nullable=False
    )
    stop_reason: Final[Column] = CharField(20)
    refills: Final[Column] = IntField()
    quantity: Final[Column] = NumericField()
    days_supply: Final[Column] = IntField()
    sig: Final[Column] = CharField(None)
    route_concept_id: Final[Column] = IntField(FK(Concept.concept_id))
    lot_number: Final[Column] = CharField(50)
    drug_source_value: Final[Column] = CharField(50)
    drug_source_concept_id: Final[Column] = IntField(FK(Concept.concept_id))
    route_source_value: Final[Column] = CharField(50)
    dose_unit_source_value: Final[Column] = CharField(50)


@register_omop_model
@freeze_instance
class ProcedureOccurrence(ModelBase, PersonIdMixin, VisitAndProviderMixin):
    """https://ohdsi.github.io/CommonDataModel/cdm54.html#PROCEDURE_OCCURRENCE"""

    __tablename__: Final[str] = "procedure_occurrence"

    procedure_occurrence_id: Final[Column] = IntField(primary_key=True)
    procedure_concept_id: Final[Column] = IntField(
        FK(Concept.concept_id), nullable=False
    )
    procedure_date: Final[Column] = DateField(nullable=False)
    procedure_datetime: Final[Column] = DateTimeField()
    procedure_end_date: Final[Column] = DateField()
    procedure_end_datetime: Final[Column] = DateTimeField()
    procedure_type_concept_id: Final[Column] = IntField(
        FK(Concept.concept_id), nullable=False
    )
    modifier_concept_id: Final[Column] = IntField(FK(Concept.concept_id))
    quantity: Final[Column] = IntField()
    procedure_source_value: Final[Column] = CharField(50)
    procedure_source_concept_id: Final[Column] = IntField(
        FK(Concept.concept_id)
    )
    modifier_source_value: Final[Column] = CharField(50)


@register_omop_model
@freeze_instance
class DeviceExposure(ModelBase, PersonIdMixin, VisitAndProviderMixin):
    """https://ohdsi.github.io/CommonDataModel/cdm54.html#DEVICE_EXPOSURE"""

    __tablename__: Final[str] = "device_exposure"

    device_exposure_id: Final[Column] = IntField(primary_key=True)
    device_concept_id: Final[Column] = IntField(
        FK(Concept.concept_id), nullable=False
    )
    device_exposure_start_date: Final[Column] = DateField(nullable=False)
    device_exposure_start_datetime: Final[Column] = DateTimeField()
    device_exposure_end_date: Final[Column] = DateField()
    device_exposure_end_datetime: Final[Column] = DateTimeField()
    device_type_concept_id: Final[Column] = IntField(
        FK(Concept.concept_id), nullable=False
    )
    unique_device_id: Final[Column] = CharField(255)
    production_id: Final[Column] = CharField(255)
    quantity: Final[Column] = IntField()
    device_source_value: Final[Column] = CharField(50)
    device_source_concept_id: Final[Column] = IntField(FK(Concept.concept_id))
    unit_concept_id: Final[Column] = IntField(FK(Concept.concept_id))
    unit_source_value: Final[Column] = CharField(50)
    unit_source_concept_id: Final[Column] = IntField(FK(Concept.concept_id))


@register_omop_model
@freeze_instance
class Measurement(ModelBase, PersonIdMixin, VisitAndProviderMixin):
    """https://ohdsi.github.io/CommonDataModel/cdm54.html#MEASUREMENT"""

    __tablename__: Final[str] = "measurement"

    measurement_id: Final[Column] = IntField(primary_key=True)
    measurement_concept_id: Final[Column] = IntField(
        FK(Concept.concept_id), nullable=False
    )
    measurement_date: Final[Column] = DateField(nullable=False)
    measurement_datetime: Final[Column] = DateTimeField()
    measurement_time: Final[Column] = CharField(10)
    measurement_type_concept_id: Final[Column] = IntField(
        FK(Concept.concept_id), nullable=False
    )
    operator_concept_id: Final[Column] = IntField(FK(Concept.concept_id))
    value_as_number: Final[Column] = NumericField()
    value_as_concept_id: Final[Column] = IntField(FK(Concept.concept_id))
    unit_concept_id: Final[Column] = IntField(FK(Concept.concept_id))
    range_low: Final[Column] = NumericField()
    range_high: Final[Column] = NumericField()
    measurement_source_value: Final[Column] = CharField(50)
    measurement_source_concept_id: Final[Column] = IntField(
        FK(Concept.concept_id)
    )
    unit_source_value: Final[Column] = CharField(50)
    unit_source_concept_id: Final[Column] = IntField(FK(Concept.concept_id))
    value_source_value: Final[Column] = CharField(50)
    measurement_event_id: Final[Column] = IntField()
    meas_event_field_concept_id: Final[Column] = IntField(
        FK(Concept.concept_id)
    )


@register_omop_model
@freeze_instance
class Observation(ModelBase, PersonIdMixin, VisitAndProviderMixin):
    """
    https://ohdsi.github.io/CommonDataModel/cdm54.html#OBSERVATION
    """

    __tablename__: Final[str] = "observation"

    observation_id: Final[Column] = IntField(primary_key=True)
    observation_concept_id: Final[Column] = IntField(
        FK(Concept.concept_id), nullable=False
    )
    observation_date: Final[Column] = DateField(nullable=False)
    observation_datetime: Final[Column] = DateTimeField()
    observation_type_concept_id: Final[Column] = IntField(
        FK(Concept.concept_id), nullable=False
    )
    value_as_number: Final[Column] = NumericField()
    value_as_string: Final[Column] = CharField(60)
    value_as_concept_id: Final[Column] = IntField(FK(Concept.concept_id))
    qualifier_concept_id: Final[Column] = IntField(FK(Concept.concept_id))
    unit_concept_id: Final[Column] = IntField(FK(Concept.concept_id))
    observation_source_value: Final[Column] = CharField(50)
    observation_source_concept_id: Final[Column] = IntField(
        FK(Concept.concept_id)
    )
    unit_source_value: Final[Column] = CharField(50)
    qualifier_source_value: Final[Column] = CharField(50)
    value_source_value: Final[Column] = CharField(50)
    observation_event_id: Final[Column] = IntField()
    obs_event_field_concept_id: Final[Column] = IntField(FK(Concept.concept_id))


@register_omop_model
@freeze_instance
class Death(ModelBase, PersonIdMixin, PKIdMixin):
    """https://ohdsi.github.io/CommonDataModel/cdm54.html#DEATH"""

    __tablename__: Final[str] = "death"

    death_date: Final[Column] = DateField(nullable=False)
    death_datetime: Final[Column] = DateTimeField()
    death_type_concept_id: Final[Column] = IntField(FK(Concept.concept_id))
    cause_concept_id: Final[Column] = IntField(FK(Concept.concept_id))
    cause_source_value: Final[Column] = CharField(50)
    cause_source_concept_id: Final[Column] = IntField(FK(Concept.concept_id))


@register_omop_model
@freeze_instance
class Note(ModelBase, PersonIdMixin, VisitAndProviderMixin):
    """https://ohdsi.github.io/CommonDataModel/cdm54.html#NOTE"""

    __tablename__: Final[str] = "note"

    note_id: Final[Column] = IntField(primary_key=True)
    note_date: Final[Column] = DateField(nullable=False)
    note_datetime: Final[Column] = DateTimeField()
    note_type_concept_id: Final[Column] = IntField(
        FK(Concept.concept_id), nullable=False
    )
    note_class_concept_id: Final[Column] = IntField(
        FK(Concept.concept_id), nullable=False
    )
    note_title: Final[Column] = CharField(250)
    note_text: Final[Column] = CharField(None)
    encoding_concept_id: Final[Column] = IntField(
        FK(Concept.concept_id), nullable=False
    )
    language_concept_id: Final[Column] = IntField(
        FK(Concept.concept_id), nullable=False
    )
    note_source_value: Final[Column] = CharField(50)
    note_event_id: Final[Column] = IntField()
    note_event_field_concept_id: Final[Column] = IntField(
        FK(Concept.concept_id)
    )


@register_omop_model
@freeze_instance
class NoteNlp(ModelBase):
    """https://ohdsi.github.io/CommonDataModel/cdm54.html#NOTE_NLP"""

    __tablename__: Final[str] = "note_nlp"

    note_nlp_id: Final[Column] = IntField(primary_key=True)
    note_id: Final[Column] = IntField(nullable=False)
    section_concept_id: Final[Column] = IntField(FK(Concept.concept_id))
    snippet: Final[Column] = CharField(250)
    offset: Final[Column] = CharField(250, key='"offset"')
    lexical_variant: Final[Column] = CharField(250, nullable=False)
    note_nlp_concept_id: Final[Column] = IntField(FK(Concept.concept_id))
    note_nlp_source_concept_id: Final[Column] = IntField(FK(Concept.concept_id))
    nlp_system: Final[Column] = CharField(250)
    nlp_date: Final[Column] = DateField(nullable=False)
    nlp_datetime: Final[Column] = DateTimeField()
    term_exists: Final[Column] = CharField(1)
    term_temporal: Final[Column] = CharField(50)
    term_modifiers: Final[Column] = CharField(2000)


@register_omop_model
@freeze_instance
class Specimen(ModelBase):
    """https://ohdsi.github.io/CommonDataModel/cdm54.html#SPECIMEN"""

    __tablename__: Final[str] = "specimen"

    specimen_id: Final[Column] = IntField(primary_key=True)
    specimen_concept_id: Final[Column] = IntField(
        FK(Concept.concept_id), nullable=False
    )
    specimen_type_concept_id: Final[Column] = IntField(
        FK(Concept.concept_id), nullable=False
    )
    specimen_date: Final[Column] = DateField(nullable=False)
    specimen_datetime: Final[Column] = DateTimeField()
    quantity: Final[Column] = NumericField()
    unit_concept_id: Final[Column] = IntField(FK(Concept.concept_id))
    anatomic_site_concept_id: Final[Column] = IntField(FK(Concept.concept_id))
    disease_status_concept_id: Final[Column] = IntField(FK(Concept.concept_id))
    specimen_source_id: Final[Column] = CharField(50)
    specimen_source_value: Final[Column] = CharField(50)
    unit_source_value: Final[Column] = CharField(50)
    anatomic_site_source_value: Final[Column] = CharField(50)
    disease_status_source_value: Final[Column] = CharField(50)


@register_omop_model
@freeze_instance
class FactRelationship(ModelBase, PKIdMixin):
    """https://ohdsi.github.io/CommonDataModel/cdm54.html#FACT_RELATIONSHIP"""

    __tablename__: Final[str] = "fact_relationship"

    domain_concept_id_1: Final[Column] = IntField(
        FK(Concept.concept_id), nullable=False
    )
    fact_id_1: Final[Column] = IntField(nullable=False)
    domain_concept_id_2: Final[Column] = IntField(
        FK(Concept.concept_id), nullable=False
    )
    fact_id_2: Final[Column] = IntField(nullable=False)
    relationship_concept_id: Final[Column] = IntField(
        FK(Concept.concept_id), nullable=False
    )


@register_omop_model
@freeze_instance
class Stem(ModelBase):
    """
    Stem table
    """

    __tablename__: Final[Column] = "stem"

    domain_id: Final[Column] = CharField(50)
    datasource: Final[Column] = CharField(50)
    stem_id: Final[Column] = IntField(primary_key=True)
    person_id: Final[Column] = IntField(FK(Person.person_id), nullable=False)
    concept_id: Final[Column] = IntField(
        FK(Concept.concept_id), nullable=True, index=True
    )
    start_date: Final[Column] = DateField()
    start_datetime: Final[Column] = DateTimeField()
    end_date: Final[Column] = DateField()
    end_datetime: Final[Column] = DateTimeField()
    type_concept_id: Final[Column] = IntField()
    provider_id: Final[Column] = IntField()
    visit_occurrence_id: Final[Column] = IntField(
        FK(VisitOccurrence.visit_occurrence_id), nullable=True
    )
    visit_detail_id: Final[Column] = IntField(
        FK(VisitDetail.visit_detail_id), nullable=True
    )
    care_site_id: Final[Column] = IntField(
        FK(CareSite.care_site_id), nullable=True
    )
    source_value: Final[Column] = CharField(
        600
    )  # this may be too small for some sources
    source_concept_id: Final[Column] = CharField(50)
    value_as_number: Final[Column] = NumericField()
    value_as_string: Final[Column] = CharField(250)
    value_as_concept_id: Final[Column] = IntField(
        FK(Concept.concept_id), nullable=True, index=True
    )
    unit_concept_id: Final[Column] = IntField(
        FK(Concept.concept_id), nullable=True, index=True
    )
    value_source_value: Final[Column] = CharField(150)
    unit_source_concept_id: Final[Column] = CharField(150)
    unit_source_value: Final[Column] = CharField(50)
    verbatim_end_date: Final[Column] = CharField(50)
    days_supply: Final[Column] = CharField(50)
    dose_unit_source_value: Final[Column] = CharField(50)
    modifier_concept_id: Final[Column] = IntField(
        FK(Concept.concept_id), nullable=True, index=True
    )
    modifier_source_value: Final[Column] = CharField(50)
    measurement_datetime: Final[Column] = DateTimeField()
    operator_concept_id: Final[Column] = IntField(
        FK(Concept.concept_id), nullable=True, index=True
    )
    quantity: Final[Column] = NumericField()
    range_low: Final[Column] = IntField()
    range_high: Final[Column] = IntField()
    stop_reason: Final[Column] = CharField(50)
    refills: Final[Column] = IntField()
    sig: Final[Column] = CharField(None)
    route_concept_id: Final[Column] = IntField(
        FK(Concept.concept_id), nullable=True, index=True
    )
    route_source_value: Final[Column] = CharField(50)
    lot_number: Final[Column] = CharField(50)
    unique_device_id: Final[Column] = IntField()
    production_id: Final[Column] = CharField(255)
    anatomic_site_concept_id: Final[Column] = IntField(
        FK(Concept.concept_id), nullable=True, index=True
    )
    disease_status_concept_id: Final[Column] = IntField(
        FK(Concept.concept_id), nullable=True, index=True
    )
    specimen_source_id: Final[Column] = CharField(50)
    anatomic_site_source_value: Final[Column] = CharField(50)
    disease_status_source_value: Final[Column] = CharField(50)
    condition_status_concept_id: Final[Column] = IntField(
        FK(Concept.concept_id), nullable=True, index=True
    )
    condition_status_source_value: Final[Column] = CharField(50)
    qualifier_concept_id: Final[Column] = IntField(
        FK(Concept.concept_id), nullable=True, index=True
    )
    qualifier_source_value: Final[Column] = CharField(50)
    event_id: Final[Column] = IntField()
    event_field_concept_id: Final[Column] = IntField()
    episode_id_source: Final[Column] = CharField(50)


__all__ = [
    "Person",
    "ObservationPeriod",
    "VisitOccurrence",
    "VisitDetail",
    "ConditionOccurrence",
    "DrugExposure",
    "ProcedureOccurrence",
    "DeviceExposure",
    "Measurement",
    "Observation",
    "Death",
    "Note",
    "NoteNlp",
    "Specimen",
    "FactRelationship",
    "Stem",
]
