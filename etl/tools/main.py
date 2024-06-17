"""Main program to run the ETL"""

import logging
import os
import traceback
from argparse import ArgumentParser
from pathlib import Path
from typing import Any, Final

from etl.loader import CSVFileLoader
from etl.models.tempmodels import TEMP_MODELS
from etl.process import run_etl
from etl.util.connection import get_connection_details
from etl.util.db import (
    is_db_connected,
    make_db_session,
    make_engine_duckdb,
    make_engine_postgres,
    session_context,
)
from etl.util.exceptions import DBConnectionException
from etl.util.files import load_config_from_file
from etl.util.logger import set_logger_verbosity

DESCRIPTION: Final[str] = "Execute the Rigshospitalet ETL."


def process_args() -> Any:
    parser = ArgumentParser(description=DESCRIPTION)
    parser.add_argument(
        "-c",
        "--conn_file",
        dest="conn_file",
        required=False,
        default="connection.json",
        help="The target database connection details.",
    )
    parser.add_argument(
        "-v",
        "--verbosity",
        dest="verbosity_level",
        required=False,
        default="INFO",
        help="The verbosity level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
    )
    parser.add_argument(
        "-r",
        "--reload_vocab",
        dest="reload_vocab",
        required=False,
        default="FALSE",
        help="Boolean value to turn the vocab load on or off.",
    )
    args = parser.parse_args()
    return args


def main() -> None:
    """
    Main entrypoint for running the ETL
    """
    args = process_args()
    MAX_MEMORY_LIMIT = "250gb"  # TODO make parameter

    csv_dir = os.path.join(Path(__file__).parent.parent.absolute(), "csv")
    verbosity = args.verbosity_level
    conn_file = args.conn_file
    reload_vocab = args.reload_vocab == "TRUE"

    cnxn = get_connection_details(load_config_from_file(conn_file))

    logger = logging.getLogger("ETL")
    set_logger_verbosity(logger, verbosity)

    logger.info("Connecting to database...")
    engine = None
    if cnxn.dbms == "postgresql":
        engine = make_engine_postgres(cnxn, implicit_returning=False)
    elif cnxn.dbms == "duckdb":
        engine = make_engine_duckdb(cnxn, memory_limit=MAX_MEMORY_LIMIT)

    if not is_db_connected(engine):
        raise DBConnectionException(
            f"Cannot connect to the database, please check configuration. {cnxn}"
        )

    try:
        session = make_db_session(engine)
        with session_context(session) as cntx:
            # If anything goes wrong, we will not commit the session
            # and closing the connection without committing will
            # constitute a rollback
            run_etl(
                cntx,
                lookup_loader=CSVFileLoader(
                    Path(csv_dir), TEMP_MODELS, delimiter=";"
                ),
                reload_vocab=reload_vocab,
            )
    except KeyboardInterrupt:
        print("\n")
        logger.error("KeyboardInterrupt detected, exiting.")
    # Exceptions will be logged
    except:  # noqa: E722
        logger.critical("Uncaught exception: %s", traceback.format_exc())


if __name__ == "__main__":
    main()
