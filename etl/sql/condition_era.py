"Condition era logic."

from sqlalchemy import insert
from sqlalchemy.sql import Insert

from ..models.omopcdm54.clinical import (
    ConditionOccurrence as OmopConditionOccurrence,
)
from ..models.omopcdm54.standardized_derived_elements import (
    ConditionEra as OmopConditionEra,
)
from ..sql.utils import get_era_select
from ..util.db import AbstractSession


def get_condition_era_insert(session: AbstractSession = None) -> Insert:
    ConditionEraSelect = get_era_select(
        clinical_table=OmopConditionOccurrence,
        key_columns=["person_id", "condition_concept_id"],
        start_column="condition_start_date",
        end_column="condition_end_date",
    )

    return insert(OmopConditionEra).from_select(
        names=[
            OmopConditionEra.person_id,
            OmopConditionEra.condition_concept_id,
            OmopConditionEra.condition_era_start_date,
            OmopConditionEra.condition_era_end_date,
            OmopConditionEra.condition_occurrence_count,
        ],
        select=session.query(ConditionEraSelect.subquery()),
        include_defaults=False,
    )
