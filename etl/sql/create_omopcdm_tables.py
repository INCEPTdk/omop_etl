"""Create the omopcdm tables"""

from typing import Final, List

from ..models.modelutils import (
    DIALECT_POSTGRES,
    create_tables_sql,
    drop_tables_sql,
)
from ..models.omopcdm54 import (
    CareSite,
    CDMSource,
    Cohort,
    CohortDefinition,
    ConditionEra,
    ConditionOccurrence,
    Cost,
    Death,
    DeviceExposure,
    DoseEra,
    DrugEra,
    DrugExposure,
    Episode,
    EpisodeEvent,
    FactRelationship,
    Location,
    Measurement,
    Metadata,
    Note,
    NoteNlp,
    Observation,
    ObservationPeriod,
    PayerPlanPeriod,
    Person,
    ProcedureOccurrence,
    Provider,
    Specimen,
    Stem,
    VisitDetail,
    VisitOccurrence,
)
from ..models.omopcdm54.registry import TARGET_SCHEMA
from ..util.sql import clean_sql

MODELS: Final[List] = [
    Person,
    Location,
    Death,
    CareSite,
    Measurement,
    ConditionOccurrence,
    Observation,
    CDMSource,
    DrugEra,
    ConditionEra,
    Provider,
    ProcedureOccurrence,
    DrugExposure,
    ObservationPeriod,
    VisitOccurrence,
    Episode,
    EpisodeEvent,
    VisitDetail,
    DeviceExposure,
    DoseEra,
    Specimen,
    Note,
    NoteNlp,
    FactRelationship,
    Cost,
    PayerPlanPeriod,
    Metadata,
    Cohort,
    CohortDefinition,
    Stem,
]

SQL_CREATE_SCHEMA: Final[str] = f"CREATE SCHEMA IF NOT EXISTS {TARGET_SCHEMA};"


@clean_sql
def _ddl_sql() -> str:
    statements = [
        SQL_CREATE_SCHEMA,
        drop_tables_sql(MODELS, cascade=True),
        create_tables_sql(MODELS, dialect=DIALECT_POSTGRES),
    ]
    return " ".join(statements)


SQL: Final[str] = _ddl_sql()
