""" SQL logic for inserting registry data into the stem table"""

import os
from typing import Any, Final

import pandas as pd
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
from ...util.db import get_environment_variable as get_era_lookback_interval
from .utils import (
    find_unique_column_names,
    get_case_statement,
    harmonise_timezones,
    toggle_stem_transform,
)

REGISTRY_TIMEZONE: Final[str] = "Europe/Copenhagen"
DEFAULT_ERA_LOOKBACK_INTERVAL = get_era_lookback_interval(
    "CONDITION_ERA_LOOKBACK", "30 days", pd.to_timedelta
)


@toggle_stem_transform
def get_registry_stem_insert(session: Any = None, model: Any = None) -> Insert:
    """Insert registry data into the stem table.
    If ICD code is not mapped in concept_lookup_stem, it will be mapped using the concept_relationship table.
    The source ICD codes are in the SKS format, i.e. they are prefixed with the letter 'D' and do not contain any dots.

    """
    unique_start_date = find_unique_column_names(
        session, model, ConceptLookupStem, "start_date"
    )

    unique_end_date = find_unique_column_names(
        session, model, ConceptLookupStem, "end_date"
    )

    start_datetime = harmonise_timezones(
        get_case_statement(unique_start_date, model, TIMESTAMP),
        func.coalesce(ConceptLookupStem.timezone, REGISTRY_TIMEZONE),
    )

    end_datetime = harmonise_timezones(
        get_case_statement(unique_end_date, model, TIMESTAMP),
        func.coalesce(ConceptLookupStem.timezone, REGISTRY_TIMEZONE),
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
            cast(start_datetime, DATE).label("start_date"),
            start_datetime,
            cast(end_datetime, DATE).label("end_date"),
            end_datetime,
            func.coalesce(
                cast(ConceptLookupStem.type_concept_id, INT),
                CONCEPT_ID_REGISTRY,
            ).label("type_concept_id"),
            model.sks_code,
            ConceptLookupStem.uid,
            func.coalesce(
                ConceptLookupStem.era_lookback_interval,
                literal(DEFAULT_ERA_LOOKBACK_INTERVAL),
            ),
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
                func.replace(
                    Concept.concept_code, ".", ""
                )  # remove dots from the concept code
                == func.substring(
                    model.sks_code, 2
                ),  # remove the 'D' prefix from the source code
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
            OmopStem.era_lookback_interval,
            OmopStem.datasource,
        ],
        select=StemSelect,
    )
