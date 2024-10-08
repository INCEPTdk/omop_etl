"""Creates the CDM Source table"""

import logging
import os

import pandas as pd

from ..models.omopcdm54 import CDMSource as TARGET, Vocabulary
from ..util.dates import todays_date
from ..util.db import (
    AbstractSession,
    DataBaseWriterBuilder,
    get_environment_variable,
)

# Grab the logger
logger = logging.getLogger("ETL.Core.CDMSource")
SOURCE_RELEASE_DATE = get_environment_variable(
    "SOURCE_RELEASE_DATE", todays_date()
)


def get_vocabulary_version(session: AbstractSession) -> str:
    """Get the vocabulary version from
    the database.
    """
    sql = f"""
    SELECT {TARGET.vocabulary_version.key}
    FROM {str(Vocabulary.__table__)}
    WHERE {Vocabulary.vocabulary_id.key} = 'None'
    """

    try:
        result = session.execute(sql).fetchone()
    except AttributeError:
        logger.warning("Vocabulary version missing")
        return ""
    return result[0] if result else ""


def transform(session: AbstractSession) -> None:
    """
    Adds the CDM meta information to the DB.
    """
    git_commit_sha = "Unspecified"
    git_commit_tag = "Unspecified"
    # Defaults to link to current master
    cdm_etl_reference = "https://github.com/edencehealth/rigshospitalet_etl/"
    try:
        git_commit_sha = os.environ["COMMIT_SHA"]
    except KeyError:
        logger.warning("No git version set, will default to Unspecified!")
    logger.info("Git version: %s", git_commit_sha)
    try:
        git_commit_tag = os.environ["GITHUB_TAG"]
        # Link to the exact revision
        cdm_etl_reference = cdm_etl_reference + f"releases/tag/{git_commit_tag}"
    except KeyError:
        logger.warning("No git tag set, will default to Unspecified!")
    logger.info("Git tag: %s", git_commit_tag)
    vocabulary_version = get_vocabulary_version(session)

    cdm_source_data = {
        TARGET.cdm_source_name.key: "Rigshospitalet 1301011 ICU ETL",
        TARGET.cdm_source_abbreviation.key: "RH1301011-ICU-ETL",
        TARGET.source_description.key: "CIS data from BTH + LPR from registry data + lab data from LABKA and BCC",
        TARGET.cdm_holder.key: "Rigshospitalet",
        TARGET.source_documentation_reference.key: None,
        TARGET.cdm_etl_reference.key: cdm_etl_reference,
        TARGET.source_release_date.key: SOURCE_RELEASE_DATE,
        # TO-DO: cdm_release_date
        TARGET.cdm_release_date.key: todays_date(),
        TARGET.cdm_version.key: "5.4",
        TARGET.vocabulary_version.key: vocabulary_version,
        # TO-DO: cdm_version_concept_id
        TARGET.cdm_version_concept_id.key: 0,
    }
    output_df = pd.Series(cdm_source_data).to_frame().T

    # write to session
    writer = DataBaseWriterBuilder().build().set_source(TARGET, output_df)
    writer.write(session)
