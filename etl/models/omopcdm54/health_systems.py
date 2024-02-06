"""health system models for OMOPCDM"""
# pylint: disable=invalid-name
from typing import Final

from ...util.freeze import freeze_instance
from ..modelutils import FK, CharField, Column, IntField, NumericField
from .registry import OmopCdmModelBase as ModelBase, register_omop_model
from .vocabulary import Concept


@register_omop_model
@freeze_instance
class Location(ModelBase):
    """
    https://ohdsi.github.io/CommonDataModel/cdm54.html#LOCATION
    """

    __tablename__: Final[str] = "location"

    location_id: Final[Column] = IntField(primary_key=True)
    address_1: Final[Column] = CharField(50)
    address_2: Final[Column] = CharField(50)
    city: Final[Column] = CharField(50)
    state: Final[Column] = CharField(2)
    zip: Final[Column] = CharField(9)
    county: Final[Column] = CharField(20)
    location_source_value: Final[Column] = CharField(50)
    country_concept_id: Final[Column] = IntField(FK(Concept.concept_id))
    country_source_value: Final[Column] = CharField(80)
    latitude: Final[Column] = NumericField()
    longitude: Final[Column] = NumericField()


@register_omop_model
@freeze_instance
class CareSite(ModelBase):
    """
    https://ohdsi.github.io/CommonDataModel/cdm54.html#CARE_SITE
    """

    __tablename__: Final[str] = "care_site"

    care_site_id: Final[Column] = IntField(primary_key=True)
    care_site_name: Final[Column] = CharField(255)
    place_of_service_concept_id: Final[Column] = IntField(
        FK(Concept.concept_id)
    )
    location_id: Final[Column] = IntField(FK(Location.location_id))
    care_site_source_value: Final[Column] = CharField(50)
    place_of_service_source_value: Final[Column] = CharField(50)


@register_omop_model
@freeze_instance
class Provider(ModelBase):
    """
    https://ohdsi.github.io/CommonDataModel/cdm54.html#PROVIDER
    """

    __tablename__: Final[str] = "provider"

    provider_id: Final[Column] = IntField(primary_key=True)
    provider_name: Final[Column] = CharField(255)
    npi: Final[Column] = CharField(20)
    dea: Final[Column] = CharField(20)
    specialty_concept_id: Final[Column] = IntField(FK(Concept.concept_id))
    care_site_id: Final[Column] = IntField(FK(CareSite.care_site_id))
    year_of_birth: Final[Column] = IntField()
    gender_concept_id: Final[Column] = IntField(FK(Concept.concept_id))
    provider_source_value: Final[Column] = CharField(50)
    specialty_source_value: Final[Column] = CharField(50)
    specialty_source_concept_id: Final[Column] = IntField(
        FK(Concept.concept_id)
    )
    gender_source_value: Final[Column] = CharField(50)
    gender_source_concept_id: Final[Column] = IntField(FK(Concept.concept_id))


__all__ = [
    "Location",
    "CareSite",
    "Provider",
]
