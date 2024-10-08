"""Run the ETL and supporting classes for transformations"""

import logging
import os
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple, Union

from etl.sql.cdm_summary import log_transform_to_summary_table

from .loader import Loader
from .models.omopcdm54 import (
    CareSite,
    CDMSource,
    ConditionEra,
    ConditionOccurrence,
    Death,
    DeviceExposure,
    DrugEra,
    DrugExposure,
    Location,
    Measurement,
    Observation,
    ObservationPeriod,
    OmopCdmModelBase,
    Person,
    ProcedureOccurrence,
    Specimen,
    Stem,
    VisitOccurrence,
)
from .models.omopcdm54.registry import TARGET_SCHEMA
from .models.tempmodels import ConceptLookup, ConceptLookupStem
from .transform.care_site import transform as care_site_transform
from .transform.cdm_source import transform as cdm_source_transform
from .transform.condition_era import transform as condition_era_transform
from .transform.condition_occurrence import (
    transform as condition_occurrence_transform,
)
from .transform.create_lookup_tables import transform as create_lookup_tables
from .transform.create_omopcdm_tables import transform as create_omop_tables
from .transform.death import transform as death_transform
from .transform.device_exposure import transform as device_exposure_transform
from .transform.drug_era import transform as drug_era_transform
from .transform.drug_exposure import transform as drug_exposure_transform
from .transform.location import transform as location_transform
from .transform.measurement import transform as measurement_transform
from .transform.merge.care_site import transform as merge_care_site_transform
from .transform.merge.condition_era import (
    transform as merge_condition_era_transform,
)
from .transform.merge.death import transform as merge_death_transform
from .transform.merge.drug_era import transform as merge_drug_era_transform
from .transform.merge.observation_period import (
    transform as merge_observation_period_transform,
)
from .transform.merge.person import transform as merge_person_transform
from .transform.observation import transform as observation_transform
from .transform.observation_period import (
    transform as observation_period_transform,
)
from .transform.person import transform as person_transform
from .transform.procedure_occurrence import (
    transform as procedure_occurrence_transform,
)
from .transform.reload_vocab import transform as reload_vocab_files
from .transform.session_operation import (
    SessionOperation,
    SessionOperationDefaultMerge,
)
from .transform.specimen import transform as specimen_transform
from .transform.stem import transform as stem_transform
from .transform.visit_occurrence import transform as visit_occurrence_transform
from .util.db import AbstractSession, session_context
from .util.exceptions import ETLFatalErrorException
from .util.logger import ErrorHandler
from .util.preprocessing import (
    validate_concept_ids,
    validate_domain_ids,
    validate_era_lookback_intervals,
    validate_timezones,
)

logger = logging.getLogger("ETL.Core")
ETL_RUN_STEP = int(os.getenv("ETL_RUN_STEP", "0"))


class TransformationRegistry:
    """A simple class to manage the transformation state.
    A poor mans registry - wraps a dict basically"""

    def __init__(self, initial_state: Optional[Dict[str, Any]] = None) -> None:
        """Takes an initial state if needed"""
        self._state = initial_state if initial_state else {}

    def add_or_update(self, key: str, result: Any) -> None:
        """Add a key value pair"""
        self._state[key] = result

    def get(self, key: str) -> Union[None, Any]:
        """Get a value given a key"""
        if key not in self._state:
            return None
        return self._state[key]

    def lazy_get(self, key: str) -> Callable[[str], Any]:
        """Returns a function to retrieve a value"""

        def _get():
            return self.get(key)

        return _get


def run_transformations(
    session: AbstractSession,
    transformations: Iterable[Tuple[int, SessionOperation]],
    registry: Optional[TransformationRegistry] = None,
) -> None:
    """
    Run a collections of transformations.

    Will attempt to run all transformations first,
    it will throw ETLFatalErrorException if any error
    was raised.
    """

    ehandler = ErrorHandler()
    ehandler.reset()

    def log_and_call(j: int, trans: SessionOperation) -> Any:
        logger.info(
            "Step %s: Performing transformation %s",
            j,
            trans.description if trans.description else trans.key,
        )
        return trans(session)

    for step, operation in transformations:
        if step == -1 or ETL_RUN_STEP <= step:
            result = log_and_call(step, operation)
            if registry is not None:
                registry.add_or_update(operation.key, result)

    # check errors after all transformations have run
    # Raise an exception at the end
    if ehandler.has_error:
        raise ETLFatalErrorException(
            "A fatal error occurred for one or more transformations. Check log."
        )


def run_etl(
    session: AbstractSession, lookup_loader: Loader, reload_vocab: bool
) -> None:
    """Run the full ETL and all transformations"""
    lookup_loader.load()

    validate_concept_ids(
        lookup_loader.data.get(ConceptLookup.__tablename__),
        session,
        "concept_id",
    )
    validate_concept_ids(
        lookup_loader.data.get(ConceptLookupStem.__tablename__),
        session,
        "mapped_standard_code",
    )
    validate_domain_ids(
        lookup_loader.data.get(ConceptLookupStem.__tablename__),
        session,
        "mapped_standard_code",
        "std_code_domain",
    )
    validate_timezones(
        lookup_loader.data.get(ConceptLookupStem.__tablename__),
        session,
        "timezone",
    )
    validate_era_lookback_intervals(
        lookup_loader.data.get(ConceptLookupStem.__tablename__),
        "era_lookback_interval",
    )

    create_lookup_tables(session, lookup_loader.data)

    reload_vocab_files(session=session, reload_vocab=reload_vocab)

    registry = TransformationRegistry()

    transformations = [
        (
            -1,
            SessionOperation(
                key="create_omop",
                session=session,
                func=create_omop_tables,
                description="Create OMOP tables",
            ),
        ),
        (
            -1,
            SessionOperation(
                key=str(CDMSource.__table__),
                session=session,
                func=cdm_source_transform,
                description="CDMSource transform",
            ),
        ),
        (
            Location.__step__,
            SessionOperation(
                key=str(Location.__table__),
                session=session,
                func=location_transform,
                description="Location transform",
            ),
        ),
        (
            CareSite.__step__,
            SessionOperation(
                key=str(CareSite.__table__),
                session=session,
                func=care_site_transform,
                description="Care site transform",
            ),
        ),
        (
            Person.__step__,
            SessionOperation(
                key=str(Person.__table__),
                session=session,
                func=person_transform,
                description="Person transform",
            ),
        ),
        (
            Death.__step__,
            SessionOperation(
                key=str(Death.__table__),
                session=session,
                func=death_transform,
                description="Death transform",
            ),
        ),
        (
            VisitOccurrence.__step__,
            SessionOperation(
                key=str(VisitOccurrence.__table__),
                session=session,
                func=visit_occurrence_transform,
                description="Visit occurrence transform",
            ),
        ),
        (
            Stem.__step__,
            SessionOperation(
                key=str(Stem.__table__),
                session=session,
                func=stem_transform,
                description="Stem transform",
            ),
        ),
        (
            ConditionOccurrence.__step__,
            SessionOperation(
                key=str(ConditionOccurrence.__table__),
                session=session,
                func=condition_occurrence_transform,
                description="Condition Occurrence transform",
            ),
        ),
        (
            ProcedureOccurrence.__step__,
            SessionOperation(
                key=str(ProcedureOccurrence.__table__),
                session=session,
                func=procedure_occurrence_transform,
                description="Procedure occurrence transform",
            ),
        ),
        (
            Measurement.__step__,
            SessionOperation(
                key=str(Measurement.__table__),
                session=session,
                func=measurement_transform,
                description="Measurement transform",
            ),
        ),
        (
            DrugExposure.__step__,
            SessionOperation(
                key=str(DrugExposure.__table__),
                session=session,
                func=drug_exposure_transform,
                description="Drug exposure transform",
            ),
        ),
        (
            Observation.__step__,
            SessionOperation(
                key=str(Observation.__table__),
                session=session,
                func=observation_transform,
                description="Observation transform",
            ),
        ),
        (
            DeviceExposure.__step__,
            SessionOperation(
                key=str(DeviceExposure.__table__),
                session=session,
                func=device_exposure_transform,
                description="Device Exposure transform",
            ),
        ),
        (
            Specimen.__step__,
            SessionOperation(
                key=str(Specimen.__table__),
                session=session,
                func=specimen_transform,
                description="Specimen transform",
            ),
        ),
        (
            ObservationPeriod.__step__,
            SessionOperation(
                key=str(ObservationPeriod.__table__),
                session=session,
                func=observation_period_transform,
                description="Observation period transform",
            ),
        ),
        (
            DrugEra.__step__,
            SessionOperation(
                key=str(DrugEra.__table__),
                session=session,
                func=drug_era_transform,
                description="Drug era transform",
            ),
        ),
        (
            ConditionEra.__step__,
            SessionOperation(
                key=str(ConditionEra.__table__),
                session=session,
                func=condition_era_transform,
                description="Condition era period transform",
            ),
        ),
    ]

    run_transformations(session, transformations, registry)

    logger.info("ETL completed")
    with session_context(session) as ctx_session:
        print_summary(
            ctx_session,
            [
                Location,
                CareSite,
                Person,
                Death,
                VisitOccurrence,
                Stem,
                ConditionOccurrence,
                DrugExposure,
                Observation,
                ProcedureOccurrence,
                Measurement,
                DeviceExposure,
                Specimen,
                CDMSource,
                ObservationPeriod,
                DrugEra,
                ConditionEra,
            ],
        )


def run_merge(session: AbstractSession) -> None:
    """Run the merge ETL"""
    registry = TransformationRegistry()
    transformations = [
        (
            -1,
            SessionOperation(
                key="create_omop",
                session=session,
                func=create_omop_tables,
                description="Create OMOP tables",
            ),
        ),
        (
            -1,
            SessionOperation(
                key=str(CDMSource.__table__),
                session=session,
                func=cdm_source_transform,
                description="CDMSource transform",
            ),
        ),
        (
            Location.__step__,
            SessionOperationDefaultMerge(
                cdm_table=Location,
                session=session,
                description="Merge Location transform",
            ),
        ),
        (
            CareSite.__step__,
            SessionOperation(
                key=str(CareSite.__table__),
                session=session,
                func=merge_care_site_transform,
                description="Merge Care site transform",
            ),
        ),
        (
            Person.__step__,
            SessionOperation(
                key=str(Person.__table__),
                session=session,
                func=merge_person_transform,
                description="Merge Person transform",
            ),
        ),
        (
            Death.__step__,
            SessionOperation(
                key=str(Death.__table__),
                session=session,
                func=merge_death_transform,
                description="Merge Death transform",
            ),
        ),
        (
            VisitOccurrence.__step__,
            SessionOperationDefaultMerge(
                cdm_table=VisitOccurrence,
                session=session,
                description="Merge Visit Occurrence transform",
            ),
        ),
        (
            ConditionOccurrence.__step__,
            SessionOperationDefaultMerge(
                cdm_table=ConditionOccurrence,
                session=session,
                description="Condition Occurrence transform",
            ),
        ),
        (
            ProcedureOccurrence.__step__,
            SessionOperationDefaultMerge(
                cdm_table=ProcedureOccurrence,
                session=session,
                description="Procedure occurrence transform",
            ),
        ),
        (
            Measurement.__step__,
            SessionOperationDefaultMerge(
                cdm_table=Measurement,
                session=session,
                description="Measurement transform",
            ),
        ),
        (
            DrugExposure.__step__,
            SessionOperationDefaultMerge(
                cdm_table=DrugExposure,
                session=session,
                description="Drug transform",
            ),
        ),
        (
            Observation.__step__,
            SessionOperationDefaultMerge(
                cdm_table=Observation,
                session=session,
                description="Observation transform",
            ),
        ),
        (
            DeviceExposure.__step__,
            SessionOperationDefaultMerge(
                cdm_table=DeviceExposure,
                session=session,
                description="Device Exposure transform",
            ),
        ),
        (
            Specimen.__step__,
            SessionOperationDefaultMerge(
                cdm_table=Specimen,
                session=session,
                description="Specimen transform",
            ),
        ),
        (
            ObservationPeriod.__step__,
            SessionOperation(
                key=str(ObservationPeriod.__table__),
                session=session,
                func=merge_observation_period_transform,
                description="Observation period transform",
            ),
        ),
        (
            DrugEra.__step__,
            SessionOperation(
                key=str(DrugEra.__table__),
                session=session,
                func=merge_drug_era_transform,
                description="Drug era transform",
            ),
        ),
        (
            ConditionEra.__step__,
            SessionOperation(
                key=str(ConditionEra.__table__),
                session=session,
                func=merge_condition_era_transform,
                description="Condition era transform",
            ),
        ),
    ]
    run_transformations(session, transformations, registry)

    logger.info("ETL Merge Complete")
    with session_context(session) as ctx_session:
        print_summary(
            ctx_session,
            [
                Location,
                CareSite,
                Person,
                Death,
                VisitOccurrence,
                ConditionOccurrence,
                DrugExposure,
                Observation,
                ProcedureOccurrence,
                Measurement,
                DeviceExposure,
                Specimen,
                ObservationPeriod,
                DrugEra,
                ConditionEra,
            ],
        )


def print_summary(
    session: AbstractSession,
    models: List[OmopCdmModelBase],  # type: ignore
) -> None:
    """Print DB summary"""
    output_str = f"\n{''.join([' ' for _ in range(10)])}--- ROWS TO {TARGET_SCHEMA} OMOPCDM ---\n"
    for model in models:
        model_row_count = session.query(model).count()
        output_str += f"{model.__tablename__:>22}: {model_row_count:<20}\n"
        log_transform_to_summary_table(
            session,
            transform_name=str(model.__table__),
            model_row_count=model_row_count,
        )
    logger.info(output_str)
