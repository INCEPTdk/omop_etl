"""Run the ETL and supporting classes for transformations"""

import logging
from typing import Any, Callable, Dict, Iterable, List, Optional, Union

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
    VisitOccurrence,
)
from .models.tempmodels import ConceptLookup, ConceptLookupStem
from .transform.care_site import transform as care_site_transform
from .transform.create_lookup_tables import transform as create_lookup_tables
from .transform.create_omopcdm_tables import transform as create_omop_tables
from .transform.death import transform as death_transform
from .transform.location import transform as location_transform
from .transform.person import transform as person_transform
from .transform.reload_vocab import transform as reload_vocab_files
from .transform.session_operation import SessionOperation
from .util.db import AbstractSession
from .util.exceptions import ETLFatalErrorException
from .util.logger import ErrorHandler
from .util.preprocessing import validate_concept_ids

logger = logging.getLogger("ETL.Core")


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
    transformations: Iterable[SessionOperation],
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
            j + 1,
            trans.description if trans.description else trans.key,
        )
        return trans(session)

    for i, operation in enumerate(transformations):
        result = log_and_call(i, operation)
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

    create_lookup_tables(session, lookup_loader.data)

    reload_vocab_files(session=session, reload_vocab=reload_vocab)

    registry = TransformationRegistry()

    transformations = [
        SessionOperation(
            key="create_omop",
            session=session,
            func=create_omop_tables,
            description="Create OMOP tables",
        ),
        SessionOperation(
            key=str(Location.__table__),
            session=session,
            func=location_transform,
            description="Location transform",
        ),
        SessionOperation(
            key=str(CareSite.__table__),
            session=session,
            func=care_site_transform,
            description="Care site transform",
        ),
        SessionOperation(
            # add transformations here.....
            key=str(Person.__table__),
            session=session,
            func=person_transform,
            description="Person transform",
        ),
        SessionOperation(
            key=str(Death.__table__),
            session=session,
            func=death_transform,
            description="Death transform",
        ),
    ]

    run_transformations(session, transformations, registry)

    logger.info("ETL completed")
    print_summary(
        session,
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
            CDMSource,
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
    output_str = (
        f"\n{''.join([' ' for _ in range(10)])}--- ROWS TO OMOPCDM ---\n"
    )
    for model in models:
        output_str += (
            f"{model.__tablename__:>22}: {session.query(model).count():<20}\n"
        )
    logger.info(output_str)
