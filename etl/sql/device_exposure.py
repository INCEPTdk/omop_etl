"Device exposure logic."

from typing import Final

from sqlalchemy import Date, DateTime, and_, cast, func, insert, select
from sqlalchemy.sql import Insert, Select

from ..models.omopcdm54.clinical import (
    DeviceExposure as OmopDeviceExposure,
    Stem as OmopStem,
)

StemDeviceExposure: Final[Select] = select(
    OmopStem.person_id,
    OmopStem.concept_id,
    OmopStem.start_date,
    OmopStem.start_datetime,
    func.coalesce(OmopStem.end_date, OmopStem.start_date).label("end_date"),
    func.coalesce(
        OmopStem.end_datetime,
        cast(OmopStem.end_date, DateTime),
        OmopStem.start_datetime,
    ).label("end_datetime"),
    OmopStem.type_concept_id,
    OmopStem.unique_device_id,
    OmopStem.production_id,
    OmopStem.quantity,
    OmopStem.provider_id,
    OmopStem.visit_occurrence_id,
    OmopStem.visit_detail_id,
    OmopStem.source_value,
    OmopStem.source_concept_id,
    OmopStem.unit_concept_id,
    OmopStem.unit_source_value,
    OmopStem.unit_source_concept_id,
).where(
    and_(
        OmopStem.domain_id == "Device",
        OmopStem.concept_id.is_not(None),
        OmopStem.type_concept_id.is_not(None),
    )
)

DeviceExposureInsert: Final[Insert] = insert(OmopDeviceExposure).from_select(
    names=[
        OmopDeviceExposure.person_id,
        OmopDeviceExposure.device_concept_id,
        OmopDeviceExposure.device_exposure_start_date,
        OmopDeviceExposure.device_exposure_start_datetime,
        OmopDeviceExposure.device_exposure_end_date,
        OmopDeviceExposure.device_exposure_end_datetime,
        OmopDeviceExposure.device_type_concept_id,
        OmopDeviceExposure.unique_device_id,
        OmopDeviceExposure.production_id,
        OmopDeviceExposure.quantity,
        OmopDeviceExposure.provider_id,
        OmopDeviceExposure.visit_occurrence_id,
        OmopDeviceExposure.visit_detail_id,
        OmopDeviceExposure.device_source_value,
        OmopDeviceExposure.device_source_concept_id,
        OmopDeviceExposure.unit_concept_id,
        OmopDeviceExposure.unit_source_value,
        OmopDeviceExposure.unit_source_concept_id,
    ],
    select=StemDeviceExposure,
)
