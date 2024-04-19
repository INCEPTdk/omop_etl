""" SQL query string definition for the stem functions"""

from typing import Any, Final, List

from sqlalchemy import (
    DATE,
    FLOAT,
    INT,
    TEXT,
    TIMESTAMP,
    BigInteger,
    Column,
    Table,
    and_,
    cast,
    insert,
    literal,
    or_,
    select,
)
from sqlalchemy.sql import Insert, case, func
from sqlalchemy.sql.functions import concat

from ..models.omopcdm54.clinical import Stem as OmopStem, VisitOccurrence
from ..models.tempmodels import ConceptLookupStem

TARGET_STEM_COLUMNS: Final[list] = [
    OmopStem.domain_id,
    OmopStem.person_id,
    OmopStem.concept_id,
    OmopStem.start_date,
    OmopStem.start_datetime,
    OmopStem.type_concept_id,
    OmopStem.visit_occurrence_id,
    OmopStem.source_value,
    OmopStem.source_concept_id,
    OmopStem.value_as_number,
    OmopStem.value_as_string,
    OmopStem.value_as_concept_id,
    OmopStem.unit_concept_id,
    OmopStem.unit_source_value,
    OmopStem.modifier_concept_id,
    OmopStem.operator_concept_id,
    OmopStem.range_low,
    OmopStem.range_high,
    OmopStem.stop_reason,
    OmopStem.route_concept_id,
    OmopStem.route_source_value,
    OmopStem.datasource,
]


def create_simple_stem_insert(
    model: Any = None,
    datetime_column_name: str = None,
    value_as_number_column_name: str = None,
    value_as_string_column_name: str = None,
) -> Insert:

    if not value_as_number_column_name:
        value_as_number = None
    else:
        value_as_number = case(
            (
                ConceptLookupStem.value_type == "numerical",
                getattr(model, value_as_number_column_name),
            ),
            else_=None,
        )

    if not value_as_string_column_name:
        value_as_string = None
    else:
        value_as_string = case(
            (
                ConceptLookupStem.value_type == "categorical",
                getattr(model, value_as_string_column_name),
                # std_code_domain must be observations to have any value in value_as_string
            ),
            else_=None,
        )

    StemSelect = (
        select(
            ConceptLookupStem.std_code_domain,
            VisitOccurrence.person_id,
            cast(ConceptLookupStem.mapped_standard_code, INT).label(
                "concept_id"
            ),
            cast(getattr(model, datetime_column_name), DATE).label(
                "start_date"
            ),
            cast(getattr(model, datetime_column_name), TIMESTAMP).label(
                "start_datetime"
            ),
            cast(ConceptLookupStem.type_concept_id, INT),
            VisitOccurrence.visit_occurrence_id,
            concat(model.variable, "__", cast(model.value, TEXT)),
            ConceptLookupStem.uid,
            cast(value_as_number, FLOAT).label("value_as_number"),
            cast(value_as_string, TEXT).label("value_as_string"),
            cast(ConceptLookupStem.value_as_concept_id, INT),
            cast(ConceptLookupStem.unit_concept_id, INT),
            ConceptLookupStem.unit_source_value,
            cast(ConceptLookupStem.modifier_concept_id, INT),
            cast(ConceptLookupStem.operator_concept_id, INT),
            ConceptLookupStem.range_low,
            ConceptLookupStem.range_high,
            ConceptLookupStem.stop_reason,
            cast(ConceptLookupStem.route_concept_id, INT),
            ConceptLookupStem.route_source_value,
            literal(model.__tablename__).label("datasource"),
        )
        .select_from(model)
        .join(
            VisitOccurrence,
            VisitOccurrence.visit_source_value
            == concat("courseid|", model.courseid),
        )
        .outerjoin(
            ConceptLookupStem,
            or_(
                and_(
                    ConceptLookupStem.value_type == "categorical",
                    func.lower(ConceptLookupStem.source_concept_code)
                    == func.lower(concat(model.variable, "__", model.value)),
                    ConceptLookupStem.datasource == model.__tablename__,
                ),
                and_(
                    ConceptLookupStem.value_type == "numerical",
                    func.lower(ConceptLookupStem.source_variable)
                    == func.lower(model.variable),
                    ConceptLookupStem.datasource == model.__tablename__,
                ),
            ),
        )
    )

    return insert(OmopStem).from_select(
        names=TARGET_STEM_COLUMNS,
        select=StemSelect,
    )


def create_complex_stem_insert(
    model: Any = None, column_names: List[str] = None
) -> Insert:

    stacks = []
    for col_name in column_names:
        stacks.append(
            select(
                literal(col_name).label("date_name"),
                getattr(model, col_name).label("date_value"),
                model.courseid,
                model._id,  # pylint: disable=protected-access
            ).select_from(model)
        )

        temp_datetime_cols = Table(
            "temp_datetime_cols",
            Column("date_name", TEXT),
            Column("date_value", TIMESTAMP),
            Column("courseid", BigInteger),
            Column("_id", BigInteger),
            prefixes=["TEMPORARY", "ON COMMIT DROP"],
        )
        temp_datetime_cols.create()
