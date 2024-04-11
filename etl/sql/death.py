"""Death transformation logic"""

from typing import Final

from sqlalchemy import TIMESTAMP, and_, cast, insert, literal, select
from sqlalchemy.sql import Insert
from sqlalchemy.sql.functions import concat, count

from etl.models.omopcdm54.clinical import (
    Death as OmopDeath,
    Person as OmopPerson,
)
from etl.models.source import Person as SourcePerson

REGISTRY_DEATH_TYPE_CONCEPT_ID: Final[int] = 32879

MergedOmopSourcePerson = (
    select(
        OmopPerson.person_id,
        SourcePerson.d_foddato,
        SourcePerson.c_status,
        SourcePerson.d_status_hen_start,
        cast(SourcePerson.d_status_hen_start, TIMESTAMP).label(
            "death_datetime"
        ),
        literal(REGISTRY_DEATH_TYPE_CONCEPT_ID).label("death_type_concept_id"),
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
    select=select(
        MergedOmopSourcePerson.c.person_id,
        MergedOmopSourcePerson.c.d_status_hen_start,
        MergedOmopSourcePerson.c.death_datetime,
        MergedOmopSourcePerson.c.death_type_concept_id,
    ).where(
        and_(
            MergedOmopSourcePerson.c.c_status == "90",
            MergedOmopSourcePerson.c.d_status_hen_start.is_not(None),
            MergedOmopSourcePerson.c.d_foddato.is_not(None),
        )
    ),
)

DEATH_EXCLUDED: Final[count] = count(
    select(MergedOmopSourcePerson.c.person_id)
    .where(
        and_(
            MergedOmopSourcePerson.c.c_status == "90",
            MergedOmopSourcePerson.c.d_status_hen_start.is_(None),
        )
    )
    .scalar_subquery()
)
DEATH_UPLOADED: Final[count] = count(OmopDeath.person_id)
