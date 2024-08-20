""" SQL query string definition for the stem functions"""

from typing import Any

from sqlalchemy import (
    DATE,
    FLOAT,
    INT,
    TEXT,
    TIMESTAMP,
    and_,
    case,
    cast,
    insert,
    literal,
    or_,
    select,
)
from sqlalchemy.orm import aliased
from sqlalchemy.sql import Insert, func
from sqlalchemy.sql.functions import concat

from ...models.omopcdm54.clinical import Stem as OmopStem, VisitOccurrence
from ...models.tempmodels import ConceptLookup, ConceptLookupStem
from ...util.db import AbstractSession
from .utils import (
    find_unique_column_names,
    get_case_statement,
    harmonise_timezones,
    toggle_stem_transform,
)

ASSUMED_TIMEZONE_FOR_UNMAPPED_DATA = "Europe/Copenhagen"


def _get_mapped_nondrug_stem_insert(
    model: Any = None,
    concept_lookup_stem_cte: Any = None,
    unique_start_date: str = None,
    unique_end_date: str = None,
    quantity_or_value_as_number_column_name: str = None,
    value_as_string_column_name: str = None,
) -> Insert:

    quantity_or_value_as_number = get_case_statement(
        quantity_or_value_as_number_column_name,
        model,
        FLOAT,
        "numerical",
        concept_lookup_stem_cte.c,
    )

    value_as_string = get_case_statement(
        value_as_string_column_name,
        model,
        TEXT,
        "free_text",
        concept_lookup_stem_cte.c,
    )

    value_source_value = func.coalesce(
        get_case_statement(
            quantity_or_value_as_number_column_name,
            model,
            TEXT,
            "numerical",
            concept_lookup_stem_cte.c,
        ),
        value_as_string,
        cast(model.value, TEXT),
    )

    conversion = func.coalesce(
        cast(concept_lookup_stem_cte.c.conversion, FLOAT), 1.0
    )

    value_as_concept_id_from_lookup = (
        select(ConceptLookup.concept_id)
        .where(
            and_(
                ConceptLookup.concept_string == value_as_string,
                ConceptLookup.filter
                == func.array_extract(
                    func.string_split(
                        concept_lookup_stem_cte.c.source_variable, "-"
                    ),
                    1,
                ),
            )
        )
        .scalar_subquery()
    )

    start_datetime = harmonise_timezones(
        get_case_statement(unique_start_date, model, TIMESTAMP),
        concept_lookup_stem_cte.c.timezone,
    )

    end_datetime = harmonise_timezones(
        get_case_statement(unique_end_date, model, TIMESTAMP),
        concept_lookup_stem_cte.c.timezone,
    )

    cl1 = aliased(ConceptLookup)

    StemSelectMapped = (
        select(
            concept_lookup_stem_cte.c.std_code_domain.label("domain_id"),
            VisitOccurrence.person_id,
            cast(concept_lookup_stem_cte.c.mapped_standard_code, INT).label(
                "concept_id"
            ),
            cast(start_datetime, DATE).label("start_date"),
            start_datetime,
            cast(end_datetime, DATE).label("end_date"),
            end_datetime,
            cast(concept_lookup_stem_cte.c.type_concept_id, INT),
            VisitOccurrence.visit_occurrence_id,
            concat(model.variable, "__", value_source_value),
            value_source_value,
            concept_lookup_stem_cte.c.uid,
            (conversion * quantity_or_value_as_number).label(
                "quantity_or_value_as_number"
            ),
            value_as_string.label("value_as_string"),
            func.coalesce(
                cast(concept_lookup_stem_cte.c.value_as_concept_id, INT),
                value_as_concept_id_from_lookup,
            ),
            cast(concept_lookup_stem_cte.c.unit_concept_id, INT),
            concept_lookup_stem_cte.c.unit_source_value,
            concept_lookup_stem_cte.c.unit_source_concept_id,
            cast(concept_lookup_stem_cte.c.modifier_concept_id, INT),
            cast(concept_lookup_stem_cte.c.operator_concept_id, INT),
            (conversion * concept_lookup_stem_cte.c.range_low).label(
                "range_low"
            ),
            (conversion * concept_lookup_stem_cte.c.range_high).label(
                "range_high"
            ),
            concept_lookup_stem_cte.c.stop_reason,
            func.coalesce(
                cast(concept_lookup_stem_cte.c.route_concept_id, INT),
                cl1.concept_id.label("route_concept_id"),
            ),
            concept_lookup_stem_cte.c.route_source_value,
            literal(model.__tablename__).label("datasource"),
        )
        .select_from(model)
        .join(
            VisitOccurrence,
            VisitOccurrence.visit_source_value
            == concat("courseid|", model.courseid),
        )
        .join(
            concept_lookup_stem_cte,
            or_(
                and_(
                    concept_lookup_stem_cte.c.value_type == "categorical",
                    func.lower(concept_lookup_stem_cte.c.source_concept_code)
                    == func.lower(concat(model.variable, "__", model.value)),
                    concept_lookup_stem_cte.c.datasource == model.__tablename__,
                ),
                and_(
                    concept_lookup_stem_cte.c.value_type == "numerical",
                    func.lower(concept_lookup_stem_cte.c.source_variable)
                    == func.lower(model.variable),
                    concept_lookup_stem_cte.c.datasource == model.__tablename__,
                ),
                and_(
                    concept_lookup_stem_cte.c.value_type == "free_text",
                    func.lower(concept_lookup_stem_cte.c.source_variable)
                    == func.lower(model.variable),
                    concept_lookup_stem_cte.c.datasource == model.__tablename__,
                ),
            ),
        )
        .outerjoin(
            cl1,
            and_(
                concept_lookup_stem_cte.c.route_source_value
                == cl1.concept_string,
                cl1.filter == "administration_route",
            ),
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
            OmopStem.quantity_or_value_as_number,
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
        select=StemSelectMapped,
    )


def get_unmapped_nondrug_stem_insert(
    session: AbstractSession = None,
    model: Any = None,
) -> Insert:
    """Inserts unmapped data into the stem table.
    The unmapped cases can be both source variables that are not included at all in the concept lookup stem table,
    or categorical source variables that are included in the concept lookup but not with that specific categorical
    value.
    """

    unique_start_date = find_unique_column_names(
        session, model, ConceptLookupStem, "start_date"
    )

    start_datetime = harmonise_timezones(
        get_case_statement(unique_start_date, model, TIMESTAMP),
        ASSUMED_TIMEZONE_FOR_UNMAPPED_DATA,
    )

    value_source_value = cast(model.value, TEXT)

    StemSelectUnmapped = (
        select(
            VisitOccurrence.person_id,
            VisitOccurrence.visit_occurrence_id,
            cast(start_datetime, DATE).label("start_date"),
            start_datetime,
            concat(model.variable, "__", value_source_value),
            value_source_value,
            literal(model.__tablename__).label("datasource"),
        )
        .select_from(model)
        .join(
            VisitOccurrence,
            VisitOccurrence.visit_source_value
            == concat("courseid|", model.courseid),
        )
        .where(
            case(
                (
                    and_(
                        model.variable.in_(
                            select(ConceptLookupStem.source_variable).where(
                                ConceptLookupStem.value_type == "categorical"
                            )
                        ).is_(True),
                        concat(model.variable, "__", value_source_value)
                        .in_(select(ConceptLookupStem.source_concept_code))
                        .isnot(True),
                    ),
                    True,
                ),
                (
                    model.variable.in_(
                        select(ConceptLookupStem.source_variable)
                    ),
                    False,
                ),
                else_=True,
            )
        )
    )

    return insert(OmopStem).from_select(
        names=[
            OmopStem.person_id,
            OmopStem.visit_occurrence_id,
            OmopStem.start_date,
            OmopStem.start_datetime,
            OmopStem.source_value,
            OmopStem.value_source_value,
            OmopStem.datasource,
        ],
        select=StemSelectUnmapped,
    )


@toggle_stem_transform
def get_mapped_nondrug_stem_insert(
    session: AbstractSession = None,
    model: Any = None,
    concept_lookup_stem_cte: Any = None,
) -> Insert:

    unique_start_date_columns = find_unique_column_names(
        session, model, concept_lookup_stem_cte.c, "start_date"
    )

    unique_end_date_columns = find_unique_column_names(
        session, model, concept_lookup_stem_cte.c, "end_date"
    )

    unique_quantity_or_value_as_number_columns = find_unique_column_names(
        session, model, concept_lookup_stem_cte.c, "quantity_or_value_as_number"
    )

    unique_value_as_string_columns = find_unique_column_names(
        session, model, concept_lookup_stem_cte.c, "value_as_string"
    )

    return _get_mapped_nondrug_stem_insert(
        model,
        concept_lookup_stem_cte,
        unique_start_date_columns,
        unique_end_date_columns,
        unique_quantity_or_value_as_number_columns,
        unique_value_as_string_columns,
    )
