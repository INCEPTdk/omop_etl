"""Person transformation logic"""

from typing import Dict, Final

from sqlalchemy import TIMESTAMP, and_, cast, func, insert, select
from sqlalchemy.sql import Insert
from sqlalchemy.sql.elements import Case
from sqlalchemy.sql.functions import concat

from etl.csv.lookups import generate_lookup_case, get_concept_lookup_dict
from etl.models.omopcdm54.clinical import Person as OmopPerson
from etl.models.source import Person as SourcePerson

GENDER_CONCEPT_ID_LOOKUP: Final[Dict[str, int]] = get_concept_lookup_dict(
    OmopPerson.__tablename__
)

GENDER_CONCEPT_ID_CASE: Final[Case] = generate_lookup_case(
    GENDER_CONCEPT_ID_LOOKUP, SourcePerson.c_kon
)


PERSON_INSERT: Final[Insert] = insert(OmopPerson).from_select(
    names=[
        OmopPerson.gender_concept_id,
        OmopPerson.year_of_birth,
        OmopPerson.month_of_birth,
        OmopPerson.day_of_birth,
        OmopPerson.birth_datetime,
        OmopPerson.race_concept_id,
        OmopPerson.ethnicity_concept_id,
        OmopPerson.person_source_value,
        OmopPerson.gender_source_value,
    ],
    select=select(
        [
            GENDER_CONCEPT_ID_CASE,
            func.date_part("year", SourcePerson.d_foddato),
            func.date_part("month", SourcePerson.d_foddato),
            func.date_part("day", SourcePerson.d_foddato),
            cast(SourcePerson.d_foddato, TIMESTAMP),
            0,
            0,
            concat(SourcePerson.cpr_enc.key, "|", SourcePerson.cpr_enc),
            concat("gender|", SourcePerson.c_kon),
        ],
    )
    .select_from(SourcePerson)
    .where(
        and_(
            SourcePerson.c_kon.in_(GENDER_CONCEPT_ID_LOOKUP.keys()),
            SourcePerson.d_foddato.is_not(None),
        )
    ),
)
