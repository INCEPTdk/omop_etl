""" SQL query string definition for the drug-related stem functions"""

import os
from typing import Any, Final

from sqlalchemy import (
    DATE,
    FLOAT,
    INT,
    TEXT,
    TIMESTAMP,
    and_,
    case,
    cast,
    func,
    insert,
    literal,
    or_,
    select,
    union_all,
)
from sqlalchemy.orm import aliased
from sqlalchemy.sql import Insert, text
from sqlalchemy.sql.expression import null
from sqlalchemy.sql.functions import concat

from ...models.omopcdm54.clinical import Stem as OmopStem, VisitOccurrence
from ...models.omopcdm54.vocabulary import (
    Concept as OmopConcept,
    ConceptRelationship as OmopConceptRelationship,
)
from ...models.source import Administrations, Prescriptions
from ...models.tempmodels import ConceptLookup, ConceptLookupStem
from ...util.db import get_environment_variable
from .conversions import get_conversion_factor
from .recipes import get_quantity_recipe
from .utils import (
    find_unique_column_names,
    get_case_statement,
    harmonise_timezones,
    toggle_stem_transform,
)

CONCEPT_ID_EHR: Final[int] = 32817
INCLUDE_UNMAPPED_CODES = os.getenv("INCLUDE_UNMAPPED_CODES", "TRUE") == "TRUE"
DEFAULT_ERA_LOOKBACK_INTERVAL = get_environment_variable(
    "DRUG_ERA_LOOKBACK", "24 hours"
)


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

    drugs_with_data = set(
        session.scalars(select(Administrations.drug_name).distinct())
    )
    drugs_with_mappings = set(d["source_variable"] for d in drug_mappings)
    drugs_without_mappings = set(
        d for d in drugs_with_data if d not in drugs_with_mappings
    )

    drug_mappings_with_data = [
        dm for dm in drug_mappings if dm["source_variable"] in drugs_with_data
    ]

    quantity = []
    source_quantity = []
    for dmwd in drug_mappings_with_data:
        criterion = and_(
            Administrations.drug_name == dmwd["source_variable"],
            Administrations.administration_type == dmwd["drug_exposure_type"],
        )

        if str(dmwd["quantity_or_value_as_number"]).startswith("recipe__"):
            this_quantity = get_quantity_recipe(
                Administrations,
                Prescriptions,
                dmwd["drug_exposure_type"],
                dmwd["quantity_or_value_as_number"],
                logger,
            )
        else:
            this_quantity = get_case_statement(
                dmwd["quantity_or_value_as_number"],
                Administrations,
                FLOAT,
            )

        this_conversion_factor = get_conversion_factor(
            Administrations, Prescriptions, dmwd["conversion"], logger
        )

        quantity.append((criterion, this_quantity * this_conversion_factor))
        source_quantity.append((criterion, this_quantity))

    unique_end_datetime = find_unique_column_names(
        session, Administrations, ConceptLookupStem, "end_date"
    )

    timezone = case(
        (Administrations.from_file.like("3%"), "Europe/Copenhagen"),
        (Administrations.from_file.like("8%"), "UTC"),
        (Administrations.from_file.like("9%"), "UTC"),
        else_=None,
    )

    end_datetime = harmonise_timezones(
        get_case_statement(unique_end_datetime, Administrations, TIMESTAMP),
        timezone,
    )

    start_offset = case(
        (
            Administrations.from_file.like("8%"),
            text("INTERVAL 59 seconds"),
        ),
        else_=text("INTERVAL 0 seconds"),
    )

    start_datetime = end_datetime - start_offset
    administration_route = func.coalesce(
        ConceptLookupStem.route_source_value,
        Prescriptions.epaspresadmroute,
    )

    # Create SELECT statement for drugs with custom mappings
    CustomMappedSelectSql = (
        select(
            ConceptLookupStem.std_code_domain.label("domain_id"),
            VisitOccurrence.person_id,
            cast(ConceptLookupStem.mapped_standard_code, INT).label(
                "concept_id"
            ),
            cast(start_datetime, DATE).label("start_date"),
            start_datetime,
            cast(end_datetime, DATE).label("end_date"),
            end_datetime,
            cast(ConceptLookupStem.type_concept_id, INT),
            VisitOccurrence.visit_occurrence_id,
            concat(
                Administrations.drug_name,
                "__",
                cast(Administrations.value, TEXT),
            ).label("source_value"),
            func.coalesce(OmopConcept.concept_id, ConceptLookupStem.uid).label(
                "source_concept_id"
            ),
            case(*quantity, else_=null()).label("quantity_or_value_as_number"),
            case(*source_quantity, else_=null()).label("value_source_value"),
            ConceptLookup.concept_id.label("route_concept_id"),
            administration_route.label("route_source_value"),
            func.coalesce(
                ConceptLookupStem.era_lookback_interval,
                literal(DEFAULT_ERA_LOOKBACK_INTERVAL),
            ).label("era_lookback_interval"),
            concat(
                Administrations.administration_type, "_administrations"
            ).label("datasource"),
            ConceptLookupStem.unit_source_value,
            ConceptLookupStem.unit_concept_id,
        )
        .select_from(Administrations)
        .outerjoin(
            Prescriptions,
            and_(
                Prescriptions.epaspresbaseid == Prescriptions.epaspresid,
                Prescriptions.epaspresbaseid == Administrations.epaspresbaseid,
            ),
        )
        .outerjoin(
            OmopConcept,
            Prescriptions.epaspresdrugatc == OmopConcept.concept_code,
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
                ConceptLookup.concept_string == administration_route,
                ConceptLookup.filter == "administration_route",
            ),
        )
        .where(
            and_(
                Administrations.drug_name.in_(drugs_with_mappings),
                or_(
                    and_(
                        Administrations.from_file.like("3%"),
                        Administrations.administration_type == "discrete",
                        ConceptLookupStem.std_code_domain == "Drug",
                    ),
                    and_(
                        Administrations.from_file.like("8%"),
                        Administrations.administration_type == "continuous",
                        ConceptLookupStem.std_code_domain == "Drug",
                    ),
                    and_(
                        Administrations.from_file.like("9%"),
                        Administrations.administration_type == "bolus",
                        ConceptLookupStem.std_code_domain == "Drug",
                    ),
                    and_(
                        ConceptLookupStem.std_code_domain != "Drug",
                        case(
                            (
                                ConceptLookupStem.quantity_or_value_as_number
                                == "value",
                                Administrations.unit
                                == ConceptLookupStem.unit_source_value,
                            ),
                            (
                                ConceptLookupStem.quantity_or_value_as_number
                                == "value0",
                                Administrations.unit0
                                == ConceptLookupStem.unit_source_value,
                            ),
                            (
                                ConceptLookupStem.quantity_or_value_as_number
                                == "value1",
                                Administrations.unit1
                                == ConceptLookupStem.unit_source_value,
                            ),
                            else_=True,
                        ),
                    ),
                ),
            )
        )
    )

    # Create SELECT statement for drugs without custom mappings
    # (they use go straight to ingredient level)

    OmopConcept1 = aliased(OmopConcept)

    AutoMappedSelectSql = (
        select(
            literal("Drug").label("domain_id"),
            VisitOccurrence.person_id,
            OmopConceptRelationship.concept_id_2.label("concept_id"),
            cast(start_datetime, DATE).label("start_date"),
            start_datetime,
            cast(end_datetime, DATE).label("end_date"),
            end_datetime,
            literal(CONCEPT_ID_EHR).label("type_concept_id"),
            VisitOccurrence.visit_occurrence_id,
            concat(
                Administrations.drug_name,
                "__",
                cast(Administrations.value, TEXT),
            ).label("source_value"),
            OmopConceptRelationship.concept_id_2.label("source_concept_id"),
            null().label("quantity_or_value_as_number"),
            null().label("value_source_value"),
            ConceptLookup.concept_id.label("route_concept_id"),
            Prescriptions.epaspresadmroute.label("route_source_value"),
            literal(DEFAULT_ERA_LOOKBACK_INTERVAL).label(
                "era_lookback_interval"
            ),
            case(
                (
                    OmopConceptRelationship.concept_id_2.isnot(null()),
                    "automapped_administrations",
                ),
                else_="unmapped_administrations",
            ).label("datasource"),
            null().label("unit_source_value"),
            null().label("unit_concept_id"),
        )
        .select_from(Administrations)
        .join(
            VisitOccurrence,
            VisitOccurrence.visit_source_value
            == concat("courseid|", Administrations.courseid),
        )
        .join(
            Prescriptions,
            and_(
                Prescriptions.epaspresbaseid == Administrations.epaspresbaseid,
                Prescriptions.epaspresbaseid == Prescriptions.epaspresid,
            ),
            isouter=INCLUDE_UNMAPPED_CODES,
        )
        .outerjoin(
            ConceptLookup,
            and_(
                ConceptLookup.concept_string == Prescriptions.epaspresadmroute,
                ConceptLookup.filter == "administration_route",
            ),
        )
        .join(
            OmopConcept1,
            OmopConcept1.concept_code == Prescriptions.epaspresdrugatc,
            isouter=INCLUDE_UNMAPPED_CODES,
        )
        .join(
            OmopConceptRelationship,
            and_(
                OmopConceptRelationship.concept_id_1 == OmopConcept1.concept_id,
                OmopConceptRelationship.relationship_id
                == "ATC - RxNorm pr lat",
            ),
            isouter=INCLUDE_UNMAPPED_CODES,
        )
        .where(
            and_(
                Administrations.drug_name.in_(drugs_without_mappings),
                or_(
                    and_(
                        Administrations.from_file.like("3%"),
                        Administrations.administration_type == "discrete",
                    ),
                    and_(
                        Administrations.from_file.like("8%"),
                        Administrations.administration_type == "continuous",
                    ),
                    and_(
                        Administrations.from_file.like("9%"),
                        Administrations.administration_type == "bolus",
                    ),
                ),
            )
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
            OmopStem.source_concept_id,
            OmopStem.quantity_or_value_as_number,
            OmopStem.value_source_value,
            OmopStem.route_concept_id,
            OmopStem.route_source_value,
            OmopStem.era_lookback_interval,
            OmopStem.datasource,
            OmopStem.unit_source_value,
            OmopStem.unit_concept_id,
        ],
        select=union_all(CustomMappedSelectSql, AutoMappedSelectSql),
        include_defaults=False,
    )
