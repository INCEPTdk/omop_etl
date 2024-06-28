"""vocabulary models for OMOPCDM"""

# pylint: disable=invalid-name
from typing import Any, Final

from ...models.omopcdm54.vocabulary import Concept
from ...util.db import get_environment_variable as get_schema_name
from ...util.freeze import freeze_instance
from ..modelutils import (
    FK,
    CharField,
    Column,
    DateField,
    IntField,
    PKIdMixin,
    make_model_base,
)

RESULTS_SCHEMA: Final[str] = get_schema_name("RESULTS_SCHEMA", "results")
ResultsModelBase: Any = make_model_base(schema=RESULTS_SCHEMA)


# do not register vocab models
# @register_analytical_model
@freeze_instance
class Cohort(ResultsModelBase, PKIdMixin):
    """
    Table Description
    ---
    The COHORT table contains records of subjects that satisfy a given set of criteria for a
    duration of time. The definition of the cohort is contained within the COHORT_DEFINITION
    table. It is listed as part of the RESULTS schema because it is a table that users of the
    database as well as tools such as ATLAS need to be able to write to. The CDM and Vocabulary
    tables are all read-only so it is suggested that the COHORT and COHORT_DEFINTION tables are
    kept in a separate schema to alleviate confusion.

    ETL Conventions
    ---
    Cohorts typically include patients diagnosed with a specific condition, patients exposed to
    a particular drug, but can also be Providers who have performed a specific Procedure. Cohort
    records must have a Start Date and an End Date, but the End Date may be set to Start Date or
    could have an applied censor date using the Observation Period Start Date. Cohort records must
    contain a Subject Id, which can refer to the Person, Provider, Visit record or Care Site though
    they are most often Person Ids. The Cohort Definition will define the type of subject through
    the subject concept id. A subject can belong (or not belong) to a cohort at any moment in time.
    A subject can only have one record in the cohort table for any moment of time, i.e. it is not
    possible for a person to contain multiple records indicating cohort membership that are
    overlapping in time.

    https://ohdsi.github.io/CommonDataModel/cdm54.html#COHORT
    """

    __tablename__: Final[str] = "cohort"

    cohort_definition_id: Final[Column] = IntField(nullable=False)
    subject_id: Final[Column] = IntField(nullable=False)

    # TO-DO: Implement the constraint that no subject_id in this table
    # can have overlapping periods in time
    cohort_start_date: Final[Column] = DateField(nullable=False)
    cohort_end_date: Final[Column] = DateField(nullable=False)


# do not register vocab models
# @register_analytical_model
@freeze_instance
class CohortDefinition(ResultsModelBase, PKIdMixin):
    """
    Table Description
    ---
    The COHORT_DEFINITION table contains records defining a Cohort derived
    from the data through the associated description and syntax and upon
    instantiation (execution of the algorithm) placed into the COHORT table.
    Cohorts are a set of subjects that satisfy a given combination of inclusion
    criteria for a duration of time. The COHORT_DEFINITION table provides a
    standardized structure for maintaining the rules governing the inclusion of
    a subject into a cohort, and can store operational programming code to instantiate
    the cohort within the OMOP Common Data Model.

    https://ohdsi.github.io/CommonDataModel/cdm54.html#COHORT_DEFINITION
    """

    __tablename__: Final[str] = "cohort_definition"

    # This is the identifier given to the cohort, usually by the ATLAS application
    cohort_definition_id: Final[Column] = IntField(nullable=False)

    # A short description of the cohort
    cohort_definition_name: Final[Column] = CharField(255, nullable=False)

    # A complete description of the cohort - VarChar(MAX)
    cohort_definition_description: Final[Column] = CharField(None)

    # Type defining what kind of Cohort Definition the record
    # represents and how the syntax may be executed.
    definition_type_concept_id: Final[Column] = IntField(
        FK(Concept.concept_id), nullable=False
    )

    # Syntax or code to operationalize the Cohort Definition.
    cohort_definition_syntax: Final[Column] = CharField(None)

    # This field contains a Concept that represents the domain of the
    # subjects that are members of the cohort (e.g., Person, Provider, Visit).
    subject_concept_id: Final[Column] = IntField(
        FK(Concept.concept_id), nullable=False
    )

    # A date to indicate when the Cohort was initiated in the COHORT table.
    cohort_initiation_date: Final[Column] = DateField()


__all__ = [
    "Cohort",
    "CohortDefinition",
]
