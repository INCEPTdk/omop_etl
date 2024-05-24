"""Program to run the Merge - ETL. combining all single ETLs targets into one."""

import logging
import traceback
from argparse import ArgumentParser
from typing import Any, Final

from etl.process import run_merge
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

DESCRIPTION: Final[str] = "Execute the Merge ETL."


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
    args = parser.parse_args()
    return args


def main() -> None:
    """
    Main entrypoint for running the MERGE.
    """
    args = process_args()

    verbosity = args.verbosity_level
    conn_file = args.conn_file

    cnxn = get_connection_details(load_config_from_file(conn_file))

    logger = logging.getLogger("MERGE")
    set_logger_verbosity(logger, verbosity)

    logger.info("Connecting to database...")
    engine = None
    if cnxn.dbms == "postgresql":
        engine = make_engine_postgres(cnxn, implicit_returning=False)
    elif cnxn.dbms == "duckdb":
        engine = make_engine_duckdb(cnxn)
    else:
        raise DBConnectionException(
            f"Unsupported DBMS: {cnxn.dbms}. Please use 'postgresql' or 'duckdb'."
        )

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
            run_merge(cntx)
    except KeyboardInterrupt:
        print("\n")
        logger.error("KeyboardInterrupt detected, exiting.")
    # Exceptions will be logged
    except:  # noqa: E722
        logger.critical("Uncaught exception: %s", traceback.format_exc())


if __name__ == "__main__":
    main()
