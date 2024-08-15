""" SQL logic for inserting registry data into the stem table"""

import os
from typing import Any

from sqlalchemy import (
    DATE,
    INT,
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

from ...models.omopcdm54.clinical import Person as OmopPerson, Stem as OmopStem
from ...models.omopcdm54.vocabulary import Concept, ConceptRelationship
from ...models.tempmodels import ConceptLookupStem
from ...sql.observation_period import CONCEPT_ID_REGISTRY
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
            func.coalesce(
                ConceptLookupStem.std_code_domain,
                Concept.domain_id,
            ).label("domain_id"),
            OmopPerson.person_id,
            func.coalesce(
                cast(ConceptLookupStem.mapped_standard_code, INT),
                ConceptRelationship.concept_id_2,
            ).label("concept_id"),
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
            func.coalesce(
                cast(ConceptLookupStem.type_concept_id, INT),
                CONCEPT_ID_REGISTRY,
            ).label("type_concept_id"),
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
        .join(
            Concept,
            and_(
                func.replace(Concept.concept_code, ".", "")
                == func.substring(model.sks_code, 2),
                Concept.vocabulary_id == "ICD10",
                or_(
                    Concept.concept_class_id == "ICD10 code",
                    Concept.concept_class_id == "ICD10 Hierarchy",
                ),
            ),
            isouter=True,
        )
        .join(
            ConceptRelationship,
            and_(
                ConceptRelationship.concept_id_1 == Concept.concept_id,
                ConceptRelationship.relationship_id == "Maps to",
            ),
            isouter=True,
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
