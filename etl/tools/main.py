"""Main program to run the ETL"""
import logging
import os
import traceback
from argparse import ArgumentParser
from datetime import datetime
from pathlib import Path
from typing import Any, Final

from etl.loader import CSVFileLoader, EmptyLoader
from etl.models.tempmodels import TEMP_MODELS
from etl.process import run_etl
from etl.util.connection import get_connection_details
from etl.util.db import (
    is_db_connected,
    make_db_session,
    make_engine_postgres,
    session_context,
)
from etl.util.exceptions import DBConnectionException
from etl.util.files import load_config_from_file

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
    args = parser.parse_args()
    return args


def setup_logger(verbosity_level: str) -> logging.Logger:
    logdir = "../log"
    if not os.path.exists(logdir):
        os.makedirs(logdir)

    logger = logging.getLogger("ETL")
    logger.setLevel(logging.DEBUG)

    c_handler = logging.StreamHandler()
    logfilename = f"etl_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    f_handler = logging.FileHandler(os.path.join(logdir, logfilename))
    l_format = logging.Formatter(
        "[%(asctime)s] [%(levelname)8s] - %(name)-25s %(message)s (%(filename)s:%(lineno)s)",
        "%Y-%m-%d %H:%M:%S",
    )
    c_handler.setFormatter(l_format)
    f_handler.setFormatter(l_format)

    log_levels = {
        "DEBUG": logging.DEBUG,
        "WARNING": logging.WARNING,
        "ERROR": logging.ERROR,
    }
    c_handler.setLevel(
        log_levels[verbosity_level]
        if verbosity_level in log_levels
        else logging.INFO
    )
    f_handler.setLevel(logging.DEBUG)

    logger.addHandler(c_handler)
    logger.addHandler(f_handler)
    return logger


def main() -> None:
    """
    Main entrypoint for running the ETL
    """
    args = process_args()

    csv_dir = os.path.join(Path(__file__).parent.parent.absolute(), "csv")
    verbosity = args.verbosity_level
    conn_file = args.conn_file

    cnxn = get_connection_details(load_config_from_file(conn_file))

    logger = setup_logger(verbosity)
    logger.info("Connecting to database...")
    engine = None
    if cnxn.dbms == "postgresql":
        engine = make_engine_postgres(cnxn, implicit_returning=False)

    if not is_db_connected(engine):
        raise DBConnectionException(
            "Cannot connect to the database, please check configuration."
        )

    try:
        session = make_db_session(engine)
        with session_context(session) as cntx:
            # If anything goes wrong, we will not commit the session
            # and closing the connection without committing will
            # constitute a rollback
            run_etl(
                cntx,
                source_loader=EmptyLoader(),
                lookup_loader=CSVFileLoader(
                    Path(csv_dir), TEMP_MODELS, delimiter=";"
                ),
            )
    except KeyboardInterrupt:
        print("\n")
        logger.error("KeyboardInterrupt detected, exiting.")
    # Exceptions will be logged
    except:  # noqa: E722
        logger.critical("Uncaught exception: %s", traceback.format_exc())


if __name__ == "__main__":
    main()
