""" SQL query string definition for the drug-related stem functions"""

import os
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
from sqlalchemy.sql.expression import CTE, null
from sqlalchemy.sql.functions import concat

from ...models.omopcdm54.clinical import Stem as OmopStem, VisitOccurrence
from ...models.source import Administrations, Prescriptions
from ...models.tempmodels import ConceptLookup, ConceptLookupStem
from .conversions import get_conversion_factor
from .recipes import get_quantity_recipe
from .utils import get_case_statement, toggle_stem_transform

INCLUDE_UNMAPPED_CODES = os.getenv("INCLUDE_UNMAPPED_CODES", "TRUE") == "TRUE"


# pylint: disable=too-many-arguments
def create_simple_stem_select(
    CtePrescriptions: Any = None,
    CteAdministrations: Any = None,
    administration_type: str = None,
    start_date_offset: str = "0 seconds",
    end_date: str = None,
    conversion_recipe: str = None,
    quantity_recipe: str = None,
    route_source_value: str = None,
    logger: Any = None,
) -> Select:
    if str(quantity_recipe).startswith("recipe__"):
        quantity_column = get_quantity_recipe(
            CteAdministrations,
            CtePrescriptions,
            administration_type,
            quantity_recipe,
            logger,
        )
    else:
        quantity_column = get_case_statement(
            quantity_recipe,
            CteAdministrations,
            FLOAT,
        )

    start_datetime = get_case_statement(
        end_date, CteAdministrations, TIMESTAMP
    ) - text(f"INTERVAL '{start_date_offset}'")

    conversion_factor = get_conversion_factor(
        CteAdministrations, CtePrescriptions, conversion_recipe, logger
    )

    StemSelect = (
        select(
            ConceptLookupStem.std_code_domain.label("domain_id"),
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
            (conversion_factor * quantity_column).label("quantity"),
            ConceptLookup.concept_id.label("route_concept_id"),
            getattr(CtePrescriptions.c, route_source_value).label(
                "route_source_value"
            ),
            ConceptLookupStem.era_lookback_interval,
            literal(f"{administration_type}_administrations").label(
                "datasource"
            ),
        )
        .select_from(CteAdministrations)
        .join(
            CtePrescriptions,
            CtePrescriptions.c.epaspresbaseid
            == CteAdministrations.c.epaspresbaseid,
        )
        .join(
            VisitOccurrence,
            VisitOccurrence.visit_source_value
            == concat("courseid|", CteAdministrations.c.courseid),
        )
        .join(
            ConceptLookupStem,
            and_(
                ConceptLookupStem.source_variable
                == CteAdministrations.c.drug_name,
                ConceptLookupStem.datasource == "administrations",
                ConceptLookupStem.drug_exposure_type == administration_type,
            ),
            isouter=INCLUDE_UNMAPPED_CODES,
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
    drug_mapping: Dict[str, Any] = None,
    CtePrescriptions: CTE = None,
    CteAdministrations: CTE = None,
    logger: Any = None,
) -> Select:
    start_datetime_offsets: Dict[str, str] = {
        "discrete": "0 seconds",
        "bolus": "0 seconds",
        "continuous": "59 seconds",
    }

    administration_type: str = drug_mapping.get("drug_exposure_type")

    return create_simple_stem_select(
        CtePrescriptions,
        CteAdministrations,
        administration_type,
        start_datetime_offsets.get(administration_type, "0 seconds"),
        drug_mapping["end_date"],
        drug_mapping["conversion"],
        drug_mapping["quantity"],
        drug_mapping["route_source_value"],
        logger,
    )


@toggle_stem_transform
def get_drug_stem_insert(session: Any = None, logger: Any = None) -> Insert:
    CtePrescriptions = (
        select(
            Prescriptions.courseid,
            Prescriptions.epaspresbaseid,
            Prescriptions.epaspresid,
            Prescriptions.epaspresdrugatc,
            Prescriptions.epaspresadmroute,
            Prescriptions.epaspresdose,
            Prescriptions.epaspresconc,
            Prescriptions.epaspresdrugunit,
            Prescriptions.epaspresdrugunitact,
            Prescriptions.epaspresmixamount,
            Prescriptions.epaspresweight,
            Prescriptions.epaspresdrugname,
        )
        .where(Prescriptions.epaspresbaseid == Prescriptions.epaspresid)
        .cte("cte_prescriptions")
    )

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

    select_stack = []
    for dmwd in drug_mappings_with_data:
        drug_name = dmwd["source_variable"]
        administration_type = dmwd["drug_exposure_type"]

        CteAdministrations = (
            session.query(
                Administrations.courseid,
                Administrations.timestamp,
                Administrations.epaspresbaseid,
                Administrations.drug_name,
                Administrations.administration_type,
                Administrations.value,
                Administrations.value0,
                Administrations.value1,
            )
            .where(
                Administrations.drug_name == drug_name,
                Administrations.administration_type == administration_type,
            )
            .cte(f"cte_administrations__{drug_name}__{administration_type}")
        )

        select_stack.append(
            get_drug_stem_select(
                dmwd, CtePrescriptions, CteAdministrations, logger
            )
        )

    MappedSelectSql = union_all(*select_stack)
    MappedSelectSql.compile(compile_kwargs={"literal_binds": True})

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
            null().label("quantity"),
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
            OmopStem.quantity,
            OmopStem.route_concept_id,
            OmopStem.route_source_value,
            OmopStem.era_lookback_interval,
            OmopStem.datasource,
        ],
        select=StemSelect,
        include_defaults=False,
    )
