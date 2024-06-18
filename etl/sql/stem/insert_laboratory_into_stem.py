""" SQL logic for inserting laboratory data into the stem table"""

import os
from typing import Any

from sqlalchemy import (
    DATE,
    FLOAT,
    INT,
    TIMESTAMP,
    VARCHAR,
    and_,
    cast,
    insert,
    literal,
    literal_column,
    select,
    union,
)
from sqlalchemy.sql import Insert, func
from sqlalchemy.sql.expression import null
from sqlalchemy.sql.functions import concat

from ...models.omopcdm54.clinical import Person as OmopPerson, Stem as OmopStem
from ...models.omopcdm54.vocabulary import Concept
from ...models.tempmodels import ConceptLookup, ConceptLookupStem
from .utils import (
    find_unique_column_names,
    get_case_statement,
    toggle_stem_transform,
)


@toggle_stem_transform
def get_laboratory_stem_insert(
    session: Any = None, model: Any = None
) -> Insert:
    unique_start_date = find_unique_column_names(
        session, model, ConceptLookupStem, "start_date"
    )

    unique_end_date = find_unique_column_names(
        session, model, ConceptLookupStem, "end_date"
    )

    value_column = find_unique_column_names(
        session, model, ConceptLookupStem, "value_as_number"
    )

    StemSelectMeasurement = (
        select(
            Concept.domain_id,
            OmopPerson.person_id,
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
            model.lab_id.label("source_value"),
            ConceptLookupStem.uid,
            literal(model.__tablename__).label("datasource"),
            ConceptLookup.concept_id.label("value_as_concept_id"),
            get_case_statement(
                value_column,
                model,
                FLOAT,
                value_type="numerical",
                lookup=ConceptLookupStem,
            ).label("value_as_number"),
            get_case_statement(value_column, model, VARCHAR).label(
                "value_source_value"
            ),
            ConceptLookupStem.unit_source_value,
            ConceptLookupStem.unit_concept_id,
            ConceptLookupStem.range_low,
            ConceptLookupStem.range_high,
        )
        .select_from(model)
        .join(
            OmopPerson,
            OmopPerson.person_source_value == concat("cpr_enc|", model.cpr_enc),
        )
        .join(
            ConceptLookupStem,
            and_(
                func.lower(ConceptLookupStem.source_variable)
                == func.lower(model.lab_id),
                ConceptLookupStem.datasource == model.__tablename__,
            ),
            isouter=os.getenv("INCLUDE_UNMAPPED_CODES", "TRUE") == "TRUE",
        )
        .outerjoin(
            ConceptLookup,
            and_(
                ConceptLookup.concept_string
                == get_case_statement(
                    value_column,
                    model,
                    VARCHAR,
                    value_type="categorical",
                    lookup=ConceptLookupStem,
                ),
                ConceptLookup.filter == "laboratory_category",
            ),
        )
        .outerjoin(
            Concept,
            Concept.concept_id == ConceptLookupStem.mapped_standard_code,
        )
    )

    StemSelectSpecimen = (
        select(
            literal_column("'Specimen'").label("domain_id"),
            OmopPerson.person_id,
            ConceptLookup.concept_id,
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
            model.lab_id.label("source_value"),
            ConceptLookupStem.uid,
            literal(model.__tablename__).label("datasource"),
            null().label("value_as_concept_id"),
            null().label("value_as_number"),
            model.system_clean.label("value_source_value"),
            null().label("unit_source_value"),
            null().label("unit_concept_id"),
            null().label("range_low"),
            null().label("range_high"),
        )
        .select_from(model)
        .join(
            OmopPerson,
            OmopPerson.person_source_value == concat("cpr_enc|", model.cpr_enc),
        )
        .join(
            ConceptLookupStem,
            and_(
                func.lower(ConceptLookupStem.source_variable)
                == func.lower(model.lab_id),
                ConceptLookupStem.datasource == model.__tablename__,
            ),
            isouter=os.getenv("INCLUDE_UNMAPPED_CODES", "TRUE") == "TRUE",
        )
        .outerjoin(
            ConceptLookup,
            and_(
                ConceptLookup.concept_string == model.system_clean,
                ConceptLookup.filter == "laboratory_system",
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
            OmopStem.source_value,
            OmopStem.source_concept_id,
            OmopStem.datasource,
            OmopStem.value_as_concept_id,
            OmopStem.value_as_number,
            OmopStem.value_source_value,
            OmopStem.unit_source_value,
            OmopStem.unit_concept_id,
            OmopStem.range_low,
            OmopStem.range_high,
        ],
        select=union(StemSelectMeasurement, StemSelectSpecimen),
        include_defaults=False,
    )
