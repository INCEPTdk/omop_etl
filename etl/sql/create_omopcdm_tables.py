"""Create the omopcdm tables"""

import os
from typing import Final, List

from ..models.modelutils import (
    DIALECT_POSTGRES,
    create_tables_sql,
    drop_tables_sql,
)
from ..models.omopcdm54 import (
    CareSite,
    CDMSource,
    CDMSummary,
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
    OmopCdmModelBase,
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
    CDMSummary,
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
ETL_RUN_STEP: Final[int] = int(os.getenv("ETL_RUN_STEP", "0"))

SQL_CREATE_SCHEMA: Final[str] = f"CREATE SCHEMA IF NOT EXISTS {TARGET_SCHEMA};"


@clean_sql
def _ddl_sql() -> str:
    models_to_create = get_models_in_scope()
    statements = [
        SQL_CREATE_SCHEMA,
        drop_tables_sql(models_to_create, cascade=True),
        create_tables_sql(models_to_create, dialect=DIALECT_POSTGRES),
    ]
    return " ".join(statements)


def get_models_in_scope() -> List[OmopCdmModelBase]:
    models = [
        m for m in MODELS if m.__step__ > ETL_RUN_STEP or m.__step__ == -1
    ]
    models = sorted(models, key=lambda m: m.__step__)
    return models


SQL: Final[str] = _ddl_sql()
