"Visit occurrence transformation logic."

from typing import Final

from sqlalchemy import (
    Date,
    DateTime,
    and_,
    cast,
    func,
    insert,
    literal,
    or_,
    select,
)
from sqlalchemy.orm import aliased
from sqlalchemy.sql import Insert, Select
from sqlalchemy.sql.expression import Case
from sqlalchemy.sql.functions import concat, count

from ..csv.lookups import SHAK_LOOKUP_DF, get_concept_lookup_dict
from ..models.omopcdm54.clinical import (
    CareSite as OmopCareSite,
    Person as OmopPerson,
    VisitOccurrence as OmopVisitOccurrence,
)
from ..models.source import CourseIdCprMapping, CourseMetadata
from ..models.tempmodels import ConceptLookup
from ..sql.care_site import get_department_info

CONCEPT_ID_EHR: Final[int] = 32817
cl1 = aliased(ConceptLookup)
cl2 = aliased(ConceptLookup)

CourseMetadataSelection = select(
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
        Case([(CourseMetadata.variable == "dischdate", CourseMetadata.value)])
    ).label("dischdate"),
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
            [
                (
                    CourseMetadata.variable == "chkouttoid",
                    CourseMetadata.value,
                )
            ]
        )
    ).label("chkouttoid"),
).group_by(CourseMetadata.courseid)

metadata_subquery_alias = CourseMetadataSelection.subquery().alias(
    "metadata_alias"
)

CourseIdMapped = (
    select(
        CourseIdCprMapping.courseid,
        CourseIdCprMapping.cpr_enc,
        metadata_subquery_alias.c.admdate,
        metadata_subquery_alias.c.admdatetime,
        metadata_subquery_alias.c.dischdate,
        metadata_subquery_alias.c.dischdt,
        metadata_subquery_alias.c.transfromid,
        metadata_subquery_alias.c.chkouttoid,
        OmopPerson.person_id,
    )
    .join(
        metadata_subquery_alias,
        metadata_subquery_alias.c.courseid == CourseIdCprMapping.courseid,
    )
    .join(
        OmopPerson,
        concat("cpr_enc|", CourseIdCprMapping.cpr_enc)
        == OmopPerson.person_source_value,
    )
)


def get_visit_occurrence_select(shak_code: str) -> Select:
    department_info = get_department_info(
        SHAK_LOOKUP_DF, shak_code, "department_type"
    )
    visit_concept_id = get_concept_lookup_dict(OmopCareSite.__tablename__)[
        department_info
    ]

    VisitOccurrenceSelectionAll = (
        select(
            CourseIdMapped.c.person_id,
            literal(visit_concept_id),
            func.coalesce(
                cast(CourseIdMapped.c.admdate, Date),
                cast(CourseIdMapped.c.admdatetime, Date),
            ).label("admdate"),
            func.coalesce(
                cast(CourseIdMapped.c.admdatetime, DateTime),
                cast(CourseIdMapped.c.admdate, DateTime),
            ).label("admdatetime"),
            func.coalesce(
                cast(CourseIdMapped.c.dischdate, Date),
                cast(CourseIdMapped.c.dischdt, Date),
            ).label("dischdate"),
            func.coalesce(
                cast(CourseIdMapped.c.dischdt, DateTime),
                cast(CourseIdMapped.c.dischdate, DateTime),
            ).label("dischdt"),
            literal(CONCEPT_ID_EHR),
            select([OmopCareSite.care_site_id])
            .where(
                OmopCareSite.care_site_source_value
                == f"department_shak_code|{shak_code}"
            )
            .scalar_subquery(),
            concat("courseid|", CourseIdMapped.c.courseid).label(
                "visit_source_value"
            ),
            cl1.concept_id,
            concat("transfromid|", CourseIdMapped.c.transfromid),
            cl2.concept_id,
            concat("chkouttoid|", CourseIdMapped.c.chkouttoid),
            func.divide(
                func.hash(concat(shak_code, "|", CourseIdMapped.c.courseid)), 2
            ),
        )
        .select_from(CourseIdMapped)
        .join(
            cl1,
            cl1.concept_string == CourseIdMapped.c.transfromid,
            isouter=True,
        )
        .join(
            cl2,
            cl2.concept_string == CourseIdMapped.c.chkouttoid,
            isouter=True,
        )
    )

    visit_occurrence_all = VisitOccurrenceSelectionAll.subquery().alias(
        "visit_occurrence_all"
    )

    return visit_occurrence_all


def get_count_courseid_missing_dates(shak_code: str) -> count:
    visit_occurrence_all = get_visit_occurrence_select(shak_code)

    return (
        select([visit_occurrence_all.c.person_id])
        .where(
            (
                (visit_occurrence_all.c.admdate.is_(None))
                & (visit_occurrence_all.c.admdatetime.is_(None))
            )
            | (
                (visit_occurrence_all.c.dischdate.is_(None))
                & (visit_occurrence_all.c.dischdt.is_(None))
            )
        )
        .subquery()
    )


def get_count_courseid_dates_not_matching(shak_code: str) -> count:
    visit_occurrence_all = get_visit_occurrence_select(shak_code)

    return (
        select([visit_occurrence_all.c.person_id])
        .where(
            or_(
                cast(visit_occurrence_all.c.admdate, Date)
                != cast(visit_occurrence_all.c.admdatetime, Date),
                cast(visit_occurrence_all.c.dischdate, Date)
                != cast(visit_occurrence_all.c.dischdt, Date),
            )
        )
        .subquery()
    )


def get_visit_occurrence_insert(shak_code: str) -> Insert:
    visit_occurrence_all = get_visit_occurrence_select(shak_code)

    VisitOccurrenceSelectionFiltered = (
        select(visit_occurrence_all)
        .where(
            and_(
                cast(visit_occurrence_all.c.admdate, Date)
                == cast(visit_occurrence_all.c.admdatetime, Date),
                cast(visit_occurrence_all.c.dischdate, Date)
                == cast(visit_occurrence_all.c.dischdt, Date),
                (
                    (visit_occurrence_all.c.admdate.isnot(None))
                    | (visit_occurrence_all.c.admdatetime.isnot(None))
                ),
                (
                    (visit_occurrence_all.c.dischdate.isnot(None))
                    | (visit_occurrence_all.c.dischdt.isnot(None))
                ),
            )
        )
        .order_by(visit_occurrence_all.c.visit_source_value)
    )

    return insert(OmopVisitOccurrence).from_select(
        names=[
            OmopVisitOccurrence.person_id,
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
            OmopVisitOccurrence.discharged_to_source_value,
            OmopVisitOccurrence.visit_occurrence_id,
        ],
        select=VisitOccurrenceSelectionFiltered,
    )
