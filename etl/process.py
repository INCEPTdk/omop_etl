"""Run the ETL and supporting classes for transformations"""

import logging
from typing import Any, Callable, Dict, Iterable, List, Optional, Union

from .loader import Loader
from .models.omopcdm54 import (
    CDMSource,
    ConditionEra,
    ConditionOccurrence,
    Death,
    DrugEra,
    DrugExposure,
    Measurement,
    Observation,
    ObservationPeriod,
    OmopCdmModelBase,
    Person,
    ProcedureOccurrence,
)
from .transform.cdm_source import transform as cdm_source_transform
from .transform.create_omopcdm_tables import transform as create_omop_tables
from .transform.session_operation import SessionOperation
from .util.db import AbstractSession
from .util.exceptions import ETLFatalErrorException
from .util.logger import ErrorHandler

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
    session: AbstractSession, source_loader: Loader, lookup_loader: Loader
) -> None:
    """Run the full ETL and all transformations"""
    source_loader.load()
    lookup_loader.load()

    registry = TransformationRegistry()

    transformations = [
        SessionOperation(
            key="create_omop",
            session=session,
            func=create_omop_tables,
            description="Create OMOP tables",
        ),
        SessionOperation(
            key=str(CDMSource.__table__),
            session=session,
            func=cdm_source_transform,
            description="CDM Source transform",
        ),
        # add transformations here.....
    ]

    run_transformations(session, transformations, registry)

    logger.info("ETL completed")
    print_summary(
        session,
        [
            CDMSource,
            Person,
            Death,
            DrugExposure,
            Measurement,
            ConditionOccurrence,
            ProcedureOccurrence,
            Observation,
            ObservationPeriod,
            DrugEra,
            ConditionEra,
        ],
    )


def print_summary(
    session: AbstractSession,
    models: List[OmopCdmModelBase],
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
