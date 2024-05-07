"""The vocabulary reload transform"""

import logging

from sqlalchemy.orm import Session

from .transformutils import execute_sql_file

logger = logging.getLogger("ETL.Reload_vocab")


def transform(session: Session, reload_vocab: bool) -> None:
    """The final load (copy from temp tables to production)"""
    logger.info("".join(["-"] * 93))
    logger.info("Launching Vocab Reloader")
    logger.info("".join(["-"] * 93))
    if reload_vocab:
        logger.info(
            "Reloading vocabulary files, setting all indexes, "
            "and constraints..."
        )
        execute_sql_file(session, "reload_vocab.sql")
        logger.info("Vocabulary Reload Step Complete!")
    else:
        logger.info("Skipping vocabulary reload!")
