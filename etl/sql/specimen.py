"""Specimen logic"""

from typing import Final

from sqlalchemy import and_, insert, select
from sqlalchemy.sql import Insert, Select

from ..models.omopcdm54.clinical import (
    Specimen as OmopSpecimen,
    Stem as OmopStem,
)

StemSpecimen: Final[Select] = select(
    OmopStem.person_id,
    OmopStem.concept_id,
    OmopStem.type_concept_id,
    OmopStem.start_date,
    OmopStem.start_datetime,
    OmopStem.quantity,
    OmopStem.unit_concept_id,
    OmopStem.anatomic_site_concept_id,
    OmopStem.disease_status_concept_id,
    OmopStem.source_concept_id,
    OmopStem.source_value,
    OmopStem.unit_source_value,
    OmopStem.anatomic_site_source_value,
    OmopStem.disease_status_source_value,
).where(
    and_(
        OmopStem.domain_id == "Specimen",
        OmopStem.concept_id.is_not(None),
        OmopStem.type_concept_id.is_not(None),
    )
)

SpecimenInsert: Final[Insert] = insert(OmopSpecimen).from_select(
    names=[
        OmopSpecimen.person_id,
        OmopSpecimen.specimen_concept_id,
        OmopSpecimen.specimen_type_concept_id,
        OmopSpecimen.specimen_date,
        OmopSpecimen.specimen_datetime,
        OmopSpecimen.quantity,
        OmopSpecimen.unit_concept_id,
        OmopSpecimen.anatomic_site_concept_id,
        OmopSpecimen.disease_status_concept_id,
        OmopSpecimen.specimen_source_id,
        OmopSpecimen.specimen_source_value,
        OmopSpecimen.unit_source_value,
        OmopSpecimen.anatomic_site_source_value,
        OmopSpecimen.disease_status_source_value,
    ],
    select=StemSpecimen,
)
