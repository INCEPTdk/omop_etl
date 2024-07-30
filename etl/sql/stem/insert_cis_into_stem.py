""" SQL query string definition for the stem functions"""

import os
from typing import Any

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
from ...models.tempmodels import ConceptLookup, ConceptLookupStem
from .utils import (
    find_unique_column_names,
    get_case_statement,
    toggle_stem_transform,
)


def create_simple_stem_insert(
    model: Any = None,
    unique_start_date: str = None,
    unique_end_date: str = None,
    value_as_number_column_name: str = None,
    value_as_string_column_name: str = None,
) -> Insert:

    value_as_number = get_case_statement(
        value_as_number_column_name,
        model,
        FLOAT,
        "numerical",
        ConceptLookupStem,
    )

    value_as_string = get_case_statement(
        value_as_string_column_name,
        model,
        TEXT,
        "free_text",
        ConceptLookupStem,
    )

    value_source_value = func.coalesce(
        get_case_statement(
            value_as_number_column_name,
            model,
            TEXT,
            "numerical",
            ConceptLookupStem,
        ),
        value_as_string,
        cast(model.value, TEXT),
    )

    conversion = func.coalesce(cast(ConceptLookupStem.conversion, FLOAT), 1.0)

    value_as_concept_id_from_lookup = (
        select(ConceptLookup.concept_id)
        .where(ConceptLookup.concept_string == value_as_string)
        .scalar_subquery()
    )

    StemSelect = (
        select(
            ConceptLookupStem.std_code_domain.label("domain_id"),
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
            concat(model.variable, "__", value_source_value),
            value_source_value,
            ConceptLookupStem.uid,
            (conversion * value_as_number).label("value_as_number"),
            value_as_string.label("value_as_string"),
            func.coalesce(
                cast(ConceptLookupStem.value_as_concept_id, INT),
                value_as_concept_id_from_lookup,
            ),
            cast(ConceptLookupStem.unit_concept_id, INT),
            ConceptLookupStem.unit_source_value,
            ConceptLookupStem.unit_source_concept_id,
            cast(ConceptLookupStem.modifier_concept_id, INT),
            cast(ConceptLookupStem.operator_concept_id, INT),
            (conversion * ConceptLookupStem.range_low).label("range_low"),
            (conversion * ConceptLookupStem.range_high).label("range_high"),
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
        .join(
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
                and_(
                    ConceptLookupStem.value_type == "free_text",
                    func.lower(ConceptLookupStem.source_variable)
                    == func.lower(model.variable),
                    ConceptLookupStem.datasource == model.__tablename__,
                ),
            ),
            isouter=os.getenv("INCLUDE_UNMAPPED_CODES", "TRUE") == "TRUE",
        )
    )

    return insert(OmopStem).from_select(
        names=[
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
            OmopStem.value_source_value,
            OmopStem.source_concept_id,
            OmopStem.value_as_number,
            OmopStem.value_as_string,
            OmopStem.value_as_concept_id,
            OmopStem.unit_concept_id,
            OmopStem.unit_source_value,
            OmopStem.unit_source_concept_id,
            OmopStem.modifier_concept_id,
            OmopStem.operator_concept_id,
            OmopStem.range_low,
            OmopStem.range_high,
            OmopStem.stop_reason,
            OmopStem.route_concept_id,
            OmopStem.route_source_value,
            OmopStem.datasource,
        ],
        select=StemSelect,
    )


@toggle_stem_transform
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
