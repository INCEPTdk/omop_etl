"Drug exposure logic."

from typing import Final

from sqlalchemy import DateTime, and_, cast, func, insert, or_, select
from sqlalchemy.sql import Insert, Select

from ..models.omopcdm54.clinical import (
    DrugExposure as OmopDrugExposure,
    Stem as OmopStem,
)

StemDrugExposure: Final[Select] = select(
    OmopStem.person_id,
    OmopStem.concept_id,
    func.coalesce(OmopStem.start_date, OmopStem.end_date).label("start_date"),
    func.coalesce(
        OmopStem.start_datetime,
        cast(OmopStem.start_date, DateTime),
        OmopStem.end_datetime,
        cast(OmopStem.end_date, DateTime),
    ).label("start_datetime"),
    func.coalesce(OmopStem.end_date, OmopStem.start_date).label("end_date"),
    func.coalesce(
        OmopStem.end_datetime,
        cast(OmopStem.end_date, DateTime),
        OmopStem.start_datetime,
        cast(OmopStem.start_date, DateTime),
    ).label("end_datetime"),
    OmopStem.type_concept_id,
    OmopStem.quantity_or_value_as_number.label("quantity"),
    OmopStem.route_concept_id,
    OmopStem.route_source_value,
    OmopStem.provider_id,
    OmopStem.visit_occurrence_id,
    OmopStem.visit_detail_id,
    OmopStem.source_value,
    OmopStem.source_concept_id,
    OmopStem.era_lookback_interval,
).where(
    and_(
        OmopStem.domain_id == "Drug",
        OmopStem.concept_id.is_not(None),
        OmopStem.type_concept_id.is_not(None),
        or_(
            OmopStem.start_date.is_not(None),
            OmopStem.end_date.is_not(None),
        ),
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
        OmopDrugExposure.era_lookback_interval,
    ],
    select=StemDrugExposure,
)
