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
    Table Description
    ---
    The LOCATION table represents a generic way to capture physical
    location or address information of Persons and Care Sites.

    ETL Conventions
    ---
    Each address or Location is unique and is present only once in the table.
    Locations do not contain names, such as the name of a hospital.
    In order to construct a full address that can be used in the postal service,
    the address information from the Location needs to be combined with
    information from the Care Site. For standardized geospatial visualization
    and analysis, addresses need to be, at the minimum be geocoded into
    latitude and longitude.

    https://ohdsi.github.io/CommonDataModel/cdm54.html#LOCATION
    """

    __tablename__: Final[str] = "location"

    # Each instance of a Location in the source data should be assigned this unique key.
    location_id: Final[Column] = IntField(primary_key=True)

    # This is the first line of the address.
    address_1: Final[Column] = CharField(50)

    # This is the second line of the address.
    address_2: Final[Column] = CharField(50)

    city: Final[Column] = CharField(50)
    state: Final[Column] = CharField(2)

    # Zip codes are handled as strings of up to 9 characters length.
    # For US addresses, these represent either a 3-digit abbreviated Zip
    # code as provided by many sources for patient protection reasons,
    # the full 5-digit Zip or the 9-digit (ZIP + 4) codes.
    # Unless for specific reasons analytical methods should expect
    # and utilize only the first 3 digits.
    # For international addresses, different rules apply.
    zip: Final[Column] = CharField(9)
    county: Final[Column] = CharField(20)

    # Put the verbatim value for the location here, as it shows up in the source.
    location_source_value: Final[Column] = CharField(50)

    # The Concept Id representing the country. Values should conform to the Geography domain.
    country_concept_id: Final[Column] = IntField(FK(Concept.concept_id))

    # The name of the country.
    country_source_value: Final[Column] = CharField(80)

    # The geocoded latitude. Must be between -90 and 90.
    latitude: Final[Column] = NumericField()

    # The geocoded longitude. Must be between -180 and 180.
    longitude: Final[Column] = NumericField()


@register_omop_model
@freeze_instance
class CareSite(ModelBase):
    """
    Table Description
    ---
    The CARE_SITE table contains a list of uniquely identified institutional
    (physical or organizational) units where healthcare delivery is practiced
    (offices, wards, hospitals, clinics, etc.).

    ETL Conventions
    ---
    Care site is a unique combination of location_id and place_of_service_source_value.
    Care site does not take into account the provider (human) information such a specialty.
    Many source data do not make a distinction between individual and institutional providers.
    The CARE_SITE table contains the institutional providers.
    If the source, instead of uniquely identifying individual Care Sites, only provides
    limited information such as Place of Service, generic or “pooled” Care Site records
    are listed in the CARE_SITE table. There can be hierarchical and business relationships
    between Care Sites. For example, wards can belong to clinics or departments, which can
    in turn belong to hospitals, which in turn can belong to hospital systems, which in turn
    can belong to HMOs.The relationships between Care Sites are defined in the
    FACT_RELATIONSHIP table.

    https://ohdsi.github.io/CommonDataModel/cdm54.html#CARE_SITE
    """

    __tablename__: Final[str] = "care_site"

    # Assign an id to each unique combination of location_id and place_of_service_source_value.
    care_site_id: Final[Column] = IntField(primary_key=True)

    # The name of the care_site as it appears in the source data
    care_site_name: Final[Column] = CharField(255)

    # This is a high-level way of characterizing a Care Site.
    # Typically, however, Care Sites can provide care in multiple settings
    # (inpatient, outpatient, etc.) and this granularity should be reflected in the visit.
    # Choose the concept in the visit domain that best represents the setting in
    # which healthcare is provided in the Care Site.
    # If most visits in a Care Site are Inpatient, then the
    # place_of_service_concept_id should represent Inpatient.
    # If information is present about a unique Care Site (e.g. Pharmacy)
    # then a Care Site record should be created.
    # If this information is not available then set to 0.
    place_of_service_concept_id: Final[Column] = IntField(
        FK(Concept.concept_id)
    )

    # The location_id from the LOCATION table representing the
    # physical location of the care_site.
    location_id: Final[Column] = IntField(FK(Location.location_id))

    # The identifier of the care_site as it appears in the source data.
    # This could be an identifier separate from the name of the care_site.
    care_site_source_value: Final[Column] = CharField(50)

    # Put the place of service of the care_site as it appears in the source data.
    place_of_service_source_value: Final[Column] = CharField(50)


@register_omop_model
@freeze_instance
class Provider(ModelBase):
    """
    Table Description
    ---
    The PROVIDER table contains a list of uniquely identified healthcare providers.
    These are individuals providing hands-on healthcare to patients, such as physicians,
    nurses, midwives, physical therapists etc.

    User Guide
    ---
    Many sources do not make a distinction between individual and institutional providers.
    The PROVIDER table contains the individual providers. If the source, instead of
    uniquely identifying individual providers, only provides limited information
    such as specialty, generic or ‘pooled’ Provider records are listed in the
    PROVIDER table.

    https://ohdsi.github.io/CommonDataModel/cdm54.html#PROVIDER
    """

    __tablename__: Final[str] = "provider"

    # It is assumed that every provider with a different unique
    # identifier is in fact a different person and should be treated
    # independently.
    # This identifier can be the original id from the source data
    # provided it is an integer, otherwise it can be an autogenerated
    # number.
    provider_id: Final[Column] = IntField(primary_key=True)

    # This field is not necessary as it is not necessary to have the
    # actual identity of the Provider. Rather, the idea is to uniquely
    # and anonymously identify providers of care across the database.
    provider_name: Final[Column] = CharField(255)

    # This is the National Provider Number issued to health care
    # providers in the US by the Centers for Medicare and
    # Medicaid Services (CMS).
    npi: Final[Column] = CharField(20)

    # This is the identifier issued by the DEA,
    # a US federal agency, that allows a provider to
    # write prescriptions for controlled substances.
    dea: Final[Column] = CharField(20)

    # This field either represents the most common specialty that
    # occurs in the data or the most specific concept that represents
    # all specialties listed, should the provider have more than one.
    # This includes physician specialties such as internal medicine,
    # emergency medicine, etc. and allied health professionals such
    # as nurses, midwives, and pharmacists.
    # NOTE: If a Provider has more than one Specialty, there are two options:
    # 1. Choose a concept_id which is a common ancestor to the multiple specialties,
    # or,
    # 2. Choose the specialty that occurs most often for the provider.
    # Concepts in this field should be Standard with a domain of Provider.
    # If not available, set to 0.
    specialty_concept_id: Final[Column] = IntField(FK(Concept.concept_id))

    # This is the CARE_SITE_ID for the location that the provider
    # primarily practices in.
    # If a Provider has more than one Care Site, the main or
    # most often exerted CARE_SITE_ID should be recorded.
    care_site_id: Final[Column] = IntField(FK(CareSite.care_site_id))

    year_of_birth: Final[Column] = IntField()

    # This field represents the recorded gender of the provider in the source data.
    # If given, put a concept from the gender domain representing the
    # recorded gender of the provider. If not available, set to 0.
    gender_concept_id: Final[Column] = IntField(FK(Concept.concept_id))

    # Use this field to link back to providers in the source data.
    # This is typically used for error checking of ETL logic.
    # Some use cases require the ability to link back to providers in the source data.
    # This field allows for the storing of the provider identifier
    # as it appears in the source.
    provider_source_value: Final[Column] = CharField(50)

    # This is the kind of provider or specialty as it appears in the source data.
    # This includes physician specialties such as internal medicine, emergency
    # medicine, etc. and allied health professionals such as nurses,
    # midwives, and pharmacists.
    # Put the kind of provider as it appears in the source data.
    # This field is up to the discretion of the ETL-er as to whether
    # this should be the coded value from the source or the
    # text description of the lookup value.
    specialty_source_value: Final[Column] = CharField(50)

    # This is often zero as many sites use proprietary codes to store physician speciality.
    # If the source data codes provider specialty in an OMOP supported vocabulary
    # store the concept_id here. If not available, set to 0.
    specialty_source_concept_id: Final[Column] = IntField(
        FK(Concept.concept_id)
    )

    # This is provider’s gender as it appears in the source data.
    # This field is up to the discretion of the ETL-er as to whether
    # this should be the coded value from the source or the
    # text description of the lookup value.
    gender_source_value: Final[Column] = CharField(50)

    # This is often zero as many sites use proprietary codes to store provider gender.
    # If the source data codes provider gender in an OMOP
    # supported vocabulary store the concept_id here. If not available, set to 0.
    gender_source_concept_id: Final[Column] = IntField(FK(Concept.concept_id))


__all__ = [
    "Location",
    "CareSite",
    "Provider",
]
