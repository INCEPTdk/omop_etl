"Drug exposure logic."

from typing import Final

from sqlalchemy import and_, insert, select
from sqlalchemy.sql import Insert, Select

from ..models.omopcdm54.clinical import (
    DrugExposure as OmopDrugExposure,
    Stem as OmopStem,
)

StemDrugExposure: Final[Select] = select(
    OmopStem.person_id,
    OmopStem.concept_id,
    OmopStem.start_date,
    OmopStem.start_datetime,
    OmopStem.end_date,
    OmopStem.end_datetime,
    OmopStem.type_concept_id,
    OmopStem.quantity,
    OmopStem.route_concept_id,
    OmopStem.route_source_value,
    OmopStem.provider_id,
    OmopStem.visit_occurrence_id,
    OmopStem.visit_detail_id,
    OmopStem.source_value,
    OmopStem.source_concept_id,
).where(
    and_(
        OmopStem.domain_id == "Drug",
        OmopStem.concept_id.is_not(None),
        OmopStem.type_concept_id.is_not(None),
    )
)

DrugExposureInsert: Final[Insert] = insert(
    OmopDrugExposure,
).from_select(
    names=[
        OmopDrugExposure.person_id,
        OmopDrugExposure.drug_concept_id,
        OmopDrugExposure.drug_exposure_start_date,
        OmopDrugExposure.drug_exposure_start_datetime,
        OmopDrugExposure.drug_exposure_end_date,
        OmopDrugExposure.drug_exposure_end_datetime,
        OmopDrugExposure.drug_type_concept_id,
        OmopDrugExposure.quantity,
        OmopDrugExposure.route_concept_id,
        OmopDrugExposure.route_source_value,
        OmopDrugExposure.provider_id,
        OmopDrugExposure.visit_occurrence_id,
        OmopDrugExposure.visit_detail_id,
        OmopDrugExposure.drug_source_value,
        OmopDrugExposure.drug_source_concept_id,
    ],
    select=StemDrugExposure,
)
