from logging import setLogRecordFactory
from ..models.source import CourseIdCprMapping
from ..models.source import CourseMetadata
from ..models.tempmodels import ConceptLookup
from ..models.omopcdm54.clinical import VisitOccurrence as OmopVisitOccurrence
from ..models.omopcdm54.clinical import CareSite
from ..csv.lookups import SHAK_LOOKUP_DF, get_concept_lookup_dict
from ..sql.care_site import get_department_info
from sqlalchemy import func, select, literal
from sqlalchemy.sql.functions import concat
from sqlalchemy.sql import Insert
from sqlalchemy.sql.expression import Case
from typing import Final
from sqlalchemy.orm import aliased

CONCEPT_ID_EHR: Final[int] = 32817

CourseMetadataSelection = (
    select(
        CourseMetadata.courseid,
        func.max(
            Case([(CourseMetadata.variable == "admdate", CourseMetadata.value)])
        ).label("admdate"),
        func.max(
            Case(
                [
                    (
                        CourseMetadata.variable == "admdatetime",
                        CourseMetadata.value,
                    )
                ]
            )
        ).label("admdatetime"),
        func.max(
            Case([(CourseMetadata.variable == "disdate", CourseMetadata.value)])
        ).label("disdate"),
        func.max(
            Case([(CourseMetadata.variable == "dischdt", CourseMetadata.value)])
        ).label("dischdt"),
        func.max(
            Case(
                [
                    (
                        CourseMetadata.variable == "transfromid",
                        CourseMetadata.value,
                    )
                ]
            )
        ).label("transfromid"),
        func.max(
            Case(
                [(CourseMetadata.variable == "chkoutoid", CourseMetadata.value)]
            )
        ).label("chkoutoid"),
    )
    .where(
        CourseMetadata.variable.in_(
            [
                "admdate",
                "admdatetime",
                "disdate",
                "dischdt",
                "transfromid",
                "chkoutoid",
            ]
        )
    )
    .group_by(CourseMetadata.courseid)
)

CourseIdMapped = select(CourseIdCprMapping).join(
    CourseMetadataSelection.subquery(),
    CourseMetadataSelection.c.courseid == CourseIdCprMapping.courseid,
)

# add count of excluded because of inconsistent data between data and datetime
# add count of data excluded because they do not have admission or discharge date
cl1 = aliased(ConceptLookup)
cl2 = aliased(ConceptLookup)


def get_visit_occurrence_insert(shak_code: str) -> Insert:
    department_info = get_department_info(
        SHAK_LOOKUP_DF, shak_code, "department_type"
    )
    visit_concept_id = get_concept_lookup_dict(CareSite.__tablename__)[
        department_info
    ]
    care_site_id = 1#get_care_site_id(shak_code)

    VISIT_OCCURRENCE_SELECTION = (
        select(
            CourseIdMapped.c.cpr_enc, #TODO this has to be personid and not cpr_enc
            literal(visit_concept_id),
            CourseMetadataSelection.c.admdate,
            CourseMetadataSelection.c.admdatetime,
            CourseMetadataSelection.c.disdate,
            CourseMetadataSelection.c.dischdt,
            literal(CONCEPT_ID_EHR),
            literal(care_site_id),
            concat("courseid|", CourseMetadataSelection.c.courseid),
            cl1.concept_id,
            concat("transfromid|", CourseMetadataSelection.c.transfromid),
            cl2.concept_id,
            concat("chkoutoid|", CourseMetadataSelection.c.chkoutoid),
        )
        .select_from(CourseIdMapped)
        .join(
            cl1,
            cl1.concept_string == CourseMetadataSelection.c.transfromid,
            isouter=True,
        )
        .join(
            cl2,
            cl2.concept_string == CourseMetadataSelection.c.chkoutoid,
            isouter=True,
        )
    )

    return Insert(OmopVisitOccurrence).from_select(
                        names=[OmopVisitOccurrence.person_id, 
                               OmopVisitOccurrence.visit_concept_id, 
                               OmopVisitOccurrence.visit_start_date,
                               OmopVisitOccurrence.visit_start_datetime, 
                               OmopVisitOccurrence.visit_end_date,
                               OmopVisitOccurrence.visit_end_datetime,
                               OmopVisitOccurrence.visit_type_concept_id,
                               OmopVisitOccurrence.care_site_id,
                               OmopVisitOccurrence.visit_source_value,
                               OmopVisitOccurrence.admitted_from_concept_id,
                               OmopVisitOccurrence.admitted_from_source_value,
                               OmopVisitOccurrence.discharged_to_concept_id,
                               OmopVisitOccurrence.discharged_to_source_value],
                        select=VISIT_OCCURRENCE_SELECTION
                        )
