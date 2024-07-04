""" SQL query string definition for the drug-related stem functions"""

import os
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
    select,
    union_all,
)
from sqlalchemy.sql import Insert, text
from sqlalchemy.sql.expression import null
from sqlalchemy.sql.functions import concat

from ...models.omopcdm54.clinical import Stem as OmopStem, VisitOccurrence
from ...models.source import Administrations, Prescriptions
from ...models.tempmodels import ConceptLookup, ConceptLookupStem
from .conversions import get_conversion_factor
from .recipes import get_quantity_recipe
from .utils import (
    find_unique_column_names,
    get_case_statement,
    toggle_stem_transform,
)

INCLUDE_UNMAPPED_CODES = os.getenv("INCLUDE_UNMAPPED_CODES", "TRUE") == "TRUE"


@toggle_stem_transform
def get_drug_stem_insert(session: Any = None, logger: Any = None) -> Insert:
    if INCLUDE_UNMAPPED_CODES:
        criterion = and_(
            ConceptLookupStem.datasource == "administrations",
            ConceptLookupStem.drug_exposure_type.isnot(None),
        )
    else:
        criterion = ConceptLookupStem.datasource == "administrations"

    drug_mappings = session.query(ConceptLookupStem).where(criterion).all()
    drug_mappings = [row.__dict__ for row in drug_mappings]

    drugs_with_data = set(session.scalars(select(Administrations.drug_name)))

    drug_mappings_with_data = [
        dm for dm in drug_mappings if dm["source_variable"] in drugs_with_data
    ]

    quantity = []
    for dmwd in drug_mappings_with_data:
        criterion = and_(
            Administrations.drug_name == dmwd["source_variable"],
            Administrations.administration_type == dmwd["drug_exposure_type"],
        )

        if str(dmwd["value_as_number"]).startswith("recipe__"):
            this_quantity = get_quantity_recipe(
                Administrations,
                Prescriptions,
                dmwd["drug_exposure_type"],
                dmwd["value_as_number"],
                logger,
            )
        else:
            this_quantity = get_case_statement(
                dmwd["value_as_number"],
                Administrations,
                FLOAT,
            )

        this_conversion_factor = get_conversion_factor(
            Administrations, Prescriptions, dmwd["conversion"], logger
        )

        quantity.append((criterion, this_quantity * this_conversion_factor))

    unique_end_datetime = find_unique_column_names(
        session, Administrations, ConceptLookupStem, "end_date"
    )

    start_datetime = get_case_statement(
        unique_end_datetime, Administrations, TIMESTAMP
    ) - case(
        (
            Administrations.administration_type == "continuous",
            text("INTERVAL 59 seconds"),
        ),
        else_=text("INTERVAL 0 seconds"),
    )

    unique_route_source_value = find_unique_column_names(
        session, Administrations, ConceptLookupStem, "route_source_value"
    )
    route_source_value = get_case_statement(
        unique_route_source_value, Prescriptions, TEXT
    )

    MappedSelectSql = (
        select(
            ConceptLookupStem.std_code_domain.label("domain_id"),
            VisitOccurrence.person_id,
            cast(ConceptLookupStem.mapped_standard_code, INT).label(
                "concept_id"
            ),
            cast(start_datetime, DATE).label("start_date"),
            cast(start_datetime, TIMESTAMP).label("start_datetime"),
            get_case_statement(
                unique_end_datetime, Administrations, DATE
            ).label("end_date"),
            get_case_statement(
                unique_end_datetime, Administrations, TIMESTAMP
            ).label("end_datetime"),
            cast(ConceptLookupStem.type_concept_id, INT),
            VisitOccurrence.visit_occurrence_id,
            concat(
                Administrations.drug_name,
                "__",
                cast(Administrations.value, TEXT),
            ).label("source_value"),
            ConceptLookupStem.uid.label("source_concept_id"),
            case(*quantity, else_=null()).label("value_as_number"),
            ConceptLookup.concept_id.label("route_concept_id"),
            route_source_value,
            ConceptLookupStem.era_lookback_interval,
            concat(
                Administrations.administration_type, "_administrations"
            ).label("datasource"),
        )
        .select_from(Administrations)
        .join(
            Prescriptions,
            and_(
                Prescriptions.epaspresbaseid == Prescriptions.epaspresid,
                Prescriptions.epaspresbaseid == Administrations.epaspresbaseid,
            ),
        )
        .join(
            VisitOccurrence,
            VisitOccurrence.visit_source_value
            == concat("courseid|", Administrations.courseid),
        )
        .join(
            ConceptLookupStem,
            and_(
                ConceptLookupStem.datasource == "administrations",
                ConceptLookupStem.source_variable == Administrations.drug_name,
                ConceptLookupStem.drug_exposure_type
                == Administrations.administration_type,
            ),
            isouter=INCLUDE_UNMAPPED_CODES,
        )
        .outerjoin(
            ConceptLookup,
            and_(
                ConceptLookup.concept_string == route_source_value,
                ConceptLookup.filter == "administration_route",
            ),
        )
    )

    CteConceptLookupStemForAntijoin = (
        select(
            ConceptLookupStem.source_variable,
            ConceptLookupStem.std_code_domain,
            ConceptLookupStem.drug_exposure_type,
        )
        .distinct()
        .cte("cte_concept_lookup_stem_for_antijoin")
    )

    UnmappedSelectSql = (
        select(
            literal("Drug").label("domain_id"),
            VisitOccurrence.person_id,
            null().label("concept_id"),
            null().label("start_date"),
            null().label("start_datetime"),
            null().label("end_date"),
            null().label("end_datetime"),
            null().label("type_concept_id"),
            VisitOccurrence.visit_occurrence_id,
            concat(
                Administrations.drug_name,
                "__",
                cast(Administrations.value, TEXT),
            ).label("source_value"),
            null().label("source_concept_id"),
            null().label("value_as_number"),
            null().label("route_concept_id"),
            null().label("route_source_value"),
            null().label("era_lookback_interval"),
            literal("unmapped_administrations").label("datasource"),
        )
        .select_from(Administrations)
        .join(
            VisitOccurrence,
            VisitOccurrence.visit_source_value
            == concat("courseid|", Administrations.courseid),
        )
        .outerjoin(
            CteConceptLookupStemForAntijoin,
            and_(
                CteConceptLookupStemForAntijoin.c.source_variable
                == Administrations.drug_name,
                CteConceptLookupStemForAntijoin.c.drug_exposure_type
                == Administrations.administration_type,
            ),
        )
        .where(CteConceptLookupStemForAntijoin.c.std_code_domain.is_(None))
    )

    if INCLUDE_UNMAPPED_CODES:
        StemSelect = union_all(MappedSelectSql, UnmappedSelectSql)
    else:
        StemSelect = MappedSelectSql

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
            OmopStem.source_concept_id,
            OmopStem.value_as_number,
            OmopStem.route_concept_id,
            OmopStem.route_source_value,
            OmopStem.era_lookback_interval,
            OmopStem.datasource,
        ],
        select=StemSelect,
        include_defaults=False,
    )
