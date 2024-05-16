""" SQL query string definition for the drug-related stem functions"""

from typing import Any, Dict

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
    select,
    union_all,
)
from sqlalchemy.sql import Insert, Select, text
from sqlalchemy.sql.expression import null
from sqlalchemy.sql.functions import concat

from ...models.omopcdm54.clinical import Stem as OmopStem, VisitOccurrence
from ...models.source import Administrations, Prescriptions
from ...models.tempmodels import ConceptLookup, ConceptLookupStem
from .utils import (
    get_case_statement,
    get_conversion_factor,
    get_quantity_recipe,
)


def create_simple_stem_select(
    CteAdministrations: Any = None,
    CtePrescriptions: Any = None,
    administration_type: str = None,
    drug_name: str = None,
    start_date_offset: str = "0 seconds",
    end_date: str = None,
    quantity: str = None,
    route_source_value: str = None,
) -> Select:
    if quantity == "recipe":
        quantity_column = get_quantity_recipe(
            CteAdministrations, CtePrescriptions, administration_type, drug_name
        )
    else:
        quantity_column = get_case_statement(
            quantity,
            CteAdministrations,
            FLOAT,
        )

    start_datetime = get_case_statement(
        end_date, CteAdministrations, TIMESTAMP
    ) - text(f"INTERVAL '{start_date_offset}'")

    conversion = get_conversion_factor(
        CteAdministrations, CtePrescriptions, drug_name
    )

    StemSelect = (
        select(
            ConceptLookupStem.std_code_domain,
            VisitOccurrence.person_id,
            cast(ConceptLookupStem.mapped_standard_code, INT).label(
                "concept_id"
            ),
            cast(start_datetime, DATE).label("start_date"),
            cast(start_datetime, TIMESTAMP).label("start_datetime"),
            get_case_statement(end_date, CteAdministrations, DATE).label(
                "end_date"
            ),
            get_case_statement(end_date, CteAdministrations, TIMESTAMP).label(
                "end_datetime"
            ),
            cast(ConceptLookupStem.type_concept_id, INT),
            VisitOccurrence.visit_occurrence_id,
            concat(
                CteAdministrations.c.drug_name,
                "__",
                cast(CteAdministrations.c.value, TEXT),
            ).label("source_value"),
            ConceptLookupStem.uid.label("source_concept_id"),
            (conversion * quantity_column).label("quantity"),
            ConceptLookup.concept_id.label("route_concept_id"),
            getattr(CtePrescriptions.c, route_source_value).label(
                "route_source_value"
            ),
            literal(f"{administration_type}_administrations").label(
                "datasource"
            ),
        )
        .select_from(CteAdministrations)
        .join(
            CtePrescriptions,
            and_(
                CtePrescriptions.c.epaspresbaseid
                == CteAdministrations.c.epaspresbaseid,
                CteAdministrations.c.administration_type == administration_type,
            ),
        )
        .join(
            VisitOccurrence,
            VisitOccurrence.visit_source_value
            == concat("courseid|", CteAdministrations.c.courseid),
        )
        .outerjoin(
            ConceptLookupStem,
            and_(
                ConceptLookupStem.source_variable
                == CteAdministrations.c.drug_name,
                ConceptLookupStem.datasource == "administrations",
            ),
        )
        .outerjoin(
            ConceptLookup,
            and_(
                ConceptLookup.concept_string
                == getattr(CtePrescriptions.c, route_source_value),
                ConceptLookup.filter == "administration_route",
            ),
        )
    )

    return StemSelect


def get_drug_stem_select(
    drug_mapping: Dict[str, Any] = None, CtePrescriptions: Any = None
) -> Select:
    drug_name: str = drug_mapping["source_variable"]

    CteAdministrationsThisDrug = (
        select(Administrations)
        .where(Administrations.drug_name == drug_name)
        .cte(f"cte_administrations_{drug_name}")
    )

    start_datetime_offsets: Dict[str, str] = {
        "discrete": "0 seconds",
        "bolus": "0 seconds",
        "continuous": "59 seconds",
    }

    administration_types: tuple = ("discrete", "bolus", "continuous")
    select_stack = []
    for administration_type in administration_types:
        select_stack.append(
            create_simple_stem_select(
                CteAdministrationsThisDrug,
                CtePrescriptions,
                administration_type,
                drug_name,
                start_datetime_offsets.get(administration_type, "0 seconds"),
                drug_mapping["end_date"],
                drug_mapping[f"quantity_{administration_type}"],
                drug_mapping["route_source_value"],
            )
        )

    return union_all(*select_stack)


def get_drug_stem_insert(session: Any = None) -> Insert:
    CtePrescriptions = (
        select(Prescriptions)
        .where(Prescriptions.epaspresbaseid == Prescriptions.epaspresid)
        .cte("cte_prescriptions")
    )

    mapped_drugs = (
        session.query(ConceptLookupStem)
        .where(ConceptLookupStem.datasource == "administrations")
        .all()
    )
    mapped_drugs = [row.__dict__ for row in mapped_drugs]
    select_stack = []
    for mp in mapped_drugs:
        select_stack.append(get_drug_stem_select(mp, CtePrescriptions))

    MappedSelectSql = union_all(*select_stack)

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
            null().label("quantity"),
            null().label("route_concept_id"),
            null().label("route_source_value"),
            literal("unmapped_administrations").label("datasource"),
        )
        .select_from(Administrations)
        .join(
            VisitOccurrence,
            VisitOccurrence.visit_source_value
            == concat("courseid|", Administrations.courseid),
        )
        .outerjoin(
            ConceptLookupStem,
            ConceptLookupStem.source_variable == Administrations.drug_name,
        )
        .where(ConceptLookupStem.std_code_domain.is_(None))
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
            OmopStem.quantity,
            OmopStem.route_concept_id,
            OmopStem.route_source_value,
            OmopStem.datasource,
        ],
        select=union_all(MappedSelectSql, UnmappedSelectSql),
    )
