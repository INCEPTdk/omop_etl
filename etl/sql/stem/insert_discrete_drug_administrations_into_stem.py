""" SQL query string definition for the stem functions"""

from typing import Any

from sqlalchemy import (
    DATE,
    FLOAT,
    INT,
    TEXT,
    TIMESTAMP,
    and_,
    cast,
    func,
    insert,
    literal,
    select,
    union_all,
)
from sqlalchemy.sql import Insert
from sqlalchemy.sql.functions import concat

from ...models.omopcdm54.clinical import Stem as OmopStem, VisitOccurrence
from ...models.source import Administrations, Prescriptions
from ...models.tempmodels import ConceptLookup, ConceptLookupStem
from .utils import CONVERSIONS, get_case_statement


def create_simple_stem_select(
    CteAdministrations: Any = None,
    drug_name: str = None,
    end_date: str = None,
    quantity: str = None,
    route_source_value: str = None,
) -> Insert:
    StemSelect = (
        select(
            ConceptLookupStem.std_code_domain,
            VisitOccurrence.person_id,
            cast(ConceptLookupStem.mapped_standard_code, INT).label(
                "concept_id"
            ),
            get_case_statement(end_date, CteAdministrations, DATE).label(
                "start_date"
            ),
            get_case_statement(end_date, CteAdministrations, TIMESTAMP).label(
                "start_datetime"
            ),
            get_case_statement(end_date, CteAdministrations, DATE).label(
                "end_date"
            ),
            get_case_statement(end_date, CteAdministrations, TIMESTAMP).label(
                "end_datetime"
            ),
            cast(ConceptLookupStem.type_concept_id, INT),
            VisitOccurrence.visit_occurrence_id,
            concat(
                CteAdministrations.c.drugname,
                "__",
                cast(CteAdministrations.c.value, TEXT),
            ).label("source_value"),
            ConceptLookupStem.uid.label("source_concept_id"),
            (
                func.coalesce(CONVERSIONS.get(drug_name), 1.0)
                * get_case_statement(quantity, CteAdministrations, FLOAT)
            ).label("quantity"),
            ConceptLookup.concept_id.label("route_concept_id"),
            getattr(Prescriptions, route_source_value).label(
                "route_source_value"
            ),
            literal("discrete_administrations").label("datasource"),
        )
        .select_from(CteAdministrations)
        .join(
            Prescriptions,
            and_(
                Prescriptions.epaspresbaseid
                == CteAdministrations.c.epaspresbaseid,
                CteAdministrations.c.drugname == drug_name,
                Prescriptions.epaspresbaseid == Prescriptions.epaspresid,
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
                == CteAdministrations.c.drugname,
                ConceptLookupStem.datasource == "administrations",
            ),
        )
        .outerjoin(
            ConceptLookup,
            and_(
                ConceptLookup.concept_string
                == getattr(Prescriptions, route_source_value),
                ConceptLookup.filter == "administration_route",
            ),
        )
    )

    return StemSelect


def get_discrete_drug_stem_insert(session: Any = None) -> Insert:
    mapped_drugs = (
        session.query(ConceptLookupStem)
        .where(
            and_(
                ConceptLookupStem.datasource == "administrations",
                ConceptLookupStem.quantity_discrete.isnot(None),
            )
        )
        .all()
    )
    mapped_drugs = [row.__dict__ for row in mapped_drugs]

    CteDiscreteAdministrations = (
        select(Administrations)
        .where(Administrations.administration_type == "discrete")
        .cte("cte_discrete_administrations")
    )

    mapped_stack = []
    for drug_mapping in mapped_drugs:
        SelectSql = create_simple_stem_select(
            CteDiscreteAdministrations,
            drug_mapping["source_variable"],
            drug_mapping["end_date"],
            drug_mapping["quantity_discrete"],
            drug_mapping["route_source_value"],
        )
        mapped_stack.append(SelectSql)
    MappedSelectSql = union_all(*mapped_stack)

    UnmappedSelectSql = (
        select(
            literal("Drug").label("domain_id"),
            VisitOccurrence.person_id,
            literal(None).label("concept_id"),
            literal(None).label("start_date"),
            literal(None).label("start_datetime"),
            literal(None).label("end_date"),
            literal(None).label("end_datetime"),
            literal(None).label("type_concept_id"),
            VisitOccurrence.visit_occurrence_id,
            concat(
                CteDiscreteAdministrations.c.drugname,
                "__",
                cast(CteDiscreteAdministrations.c.value, TEXT),
            ).label("source_value"),
            literal(None).label("source_concept_id"),
            literal(None).label("quantity"),
            literal(None).label("route_concept_id"),
            literal(None).label("route_source_value"),
            literal("discrete_administrations").label("datasource"),
        )
        .select_from(CteDiscreteAdministrations)
        .join(
            VisitOccurrence,
            VisitOccurrence.visit_source_value
            == concat("courseid|", CteDiscreteAdministrations.c.courseid),
        )
        .outerjoin(
            ConceptLookupStem,
            ConceptLookupStem.source_variable
            == CteDiscreteAdministrations.c.drugname,
        )
        .where(
            and_(ConceptLookupStem.std_code_domain.is_(None)),
            CteDiscreteAdministrations.c.administration_type == "discrete",
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
            OmopStem.quantity,
            OmopStem.route_concept_id,
            OmopStem.route_source_value,
            OmopStem.datasource,
        ],
        select=union_all(MappedSelectSql, UnmappedSelectSql),
    )
