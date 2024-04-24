""" SQL query string definition for the stem functions"""

from typing import Any, Final

from sqlalchemy import (
    DATE,
    FLOAT,
    INT,
    TEXT,
    TIMESTAMP,
    and_,
    cast,
    insert,
    literal,
    or_,
    select,
)
from sqlalchemy.sql import Insert, func
from sqlalchemy.sql.functions import concat

from ...models.omopcdm54.clinical import Stem as OmopStem, VisitOccurrence
from ...models.tempmodels import ConceptLookupStem
from .utils import find_unique_column_names, get_case_statement

TARGET_STEM_COLUMNS: Final[list] = [
    OmopStem.domain_id,
    OmopStem.person_id,
    OmopStem.concept_id,
    OmopStem.start_date,
    OmopStem.start_datetime,
    OmopStem.end_date,
    OmopStem.end_datetime,
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
    unique_start_date: str = None,
    unique_end_date: str = None,
    value_as_number_column_name: str = None,
    value_as_string_column_name: str = None,
) -> Insert:
    StemSelect = (
        select(
            ConceptLookupStem.std_code_domain,
            VisitOccurrence.person_id,
            cast(ConceptLookupStem.mapped_standard_code, INT).label(
                "concept_id"
            ),
            get_case_statement(unique_start_date, model, DATE).label(
                "start_date"
            ),
            get_case_statement(unique_start_date, model, TIMESTAMP).label(
                "start_datetime"
            ),
            get_case_statement(unique_end_date, model, DATE).label("end_date"),
            get_case_statement(unique_end_date, model, TIMESTAMP).label(
                "end_datetime"
            ),
            cast(ConceptLookupStem.type_concept_id, INT),
            VisitOccurrence.visit_occurrence_id,
            concat(model.variable, "__", cast(model.value, TEXT)),
            ConceptLookupStem.uid,
            get_case_statement(
                value_as_number_column_name,
                model,
                FLOAT,
                "numerical",
                ConceptLookupStem,
            ).label("value_as_number"),
            get_case_statement(
                value_as_string_column_name,
                model,
                TEXT,
                "categorical",
                ConceptLookupStem,
            ).label("value_as_string"),
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


def get_nondrug_stem_insert(session: Any = None, model: Any = None) -> Insert:
    unique_start_date_columns = find_unique_column_names(
        session, model, ConceptLookupStem, "start_date"
    )

    unique_end_date_columns = find_unique_column_names(
        session, model, ConceptLookupStem, "end_date"
    )

    unique_value_as_number_columns = find_unique_column_names(
        session, model, ConceptLookupStem, "value_as_number"
    )

    unique_value_as_string_columns = find_unique_column_names(
        session, model, ConceptLookupStem, "value_as_string"
    )

    return create_simple_stem_insert(
        model,
        unique_start_date_columns,
        unique_end_date_columns,
        unique_value_as_number_columns,
        unique_value_as_string_columns,
    )
