"""Death transformation logic"""

from typing import Final

from sqlalchemy import TIMESTAMP, and_, cast, insert, select
from sqlalchemy.sql import Insert
from sqlalchemy.sql.functions import concat

from etl.models.omopcdm54.clinical import (
    Death as OmopDeath,
    Person as OmopPerson,
)
from etl.models.source import Person as SourcePerson

REGISTRY_DEATH_TYPE_CONCEPT_ID: Final[int] = 32879

MergedOmopSourcePerson = (
    select(
        OmopPerson.person_id,
        SourcePerson.d_status_hen_start,
        cast(SourcePerson.d_status_hen_start, TIMESTAMP),
        REGISTRY_DEATH_TYPE_CONCEPT_ID,
    )
    .select_from(OmopPerson)
    .join(
        SourcePerson,
        concat(SourcePerson.cpr_enc.key, "|", SourcePerson.cpr_enc)
        == OmopPerson.person_source_value,
    )
)


DEATH_INSERT: Final[Insert] = insert(OmopDeath).from_select(
    names=[
        OmopDeath.person_id,
        OmopDeath.death_date,
        OmopDeath.death_datetime,
        OmopDeath.death_type_concept_id,
    ],
    select=MergedOmopSourcePerson.where(
        and_(
            SourcePerson.c_status == "90",
            SourcePerson.d_status_hen_start.is_not(None),
            SourcePerson.d_foddato.is_not(None),
        )
    ),
)
