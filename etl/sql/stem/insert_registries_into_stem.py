""" SQL logic for inserting registry data into the stem table"""

import os
from typing import Any

from sqlalchemy import DATE, INT, TIMESTAMP, and_, cast, insert, literal, select
from sqlalchemy.sql import Insert, func
from sqlalchemy.sql.functions import concat

from ...models.omopcdm54.clinical import Person as OmopPerson, Stem as OmopStem
from ...models.omopcdm54.vocabulary import Concept
from ...models.tempmodels import ConceptLookupStem
from .utils import (
    find_unique_column_names,
    get_case_statement,
    toggle_stem_transform,
)


@toggle_stem_transform
def get_registry_stem_insert(session: Any = None, model: Any = None) -> Insert:
    unique_start_date = find_unique_column_names(
        session, model, ConceptLookupStem, "start_date"
    )

    unique_end_date = find_unique_column_names(
        session, model, ConceptLookupStem, "end_date"
    )

    StemSelect = (
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
            model.sks_code,
            ConceptLookupStem.uid,
            literal(model.__tablename__).label("datasource"),
        )
        .select_from(model)
        .join(
            OmopPerson,
            OmopPerson.person_source_value == concat("cpr_enc|", model.cpr_enc),
        )
        .join(
            ConceptLookupStem,
            and_(
                ConceptLookupStem.value_type == "categorical",
                func.lower(ConceptLookupStem.source_variable)
                == func.lower(model.sks_code),
                ConceptLookupStem.datasource == model.__tablename__,
            ),
            isouter=os.getenv("INCLUDE_UNMAPPED_CODES", "TRUE") == "TRUE",
        )
        .outerjoin(
            Concept,
            Concept.concept_id == ConceptLookupStem.mapped_standard_code,
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
        ],
        select=StemSelect,
    )
