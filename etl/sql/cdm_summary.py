"SQL for writing summary of transform to table."
import types
from datetime import datetime

from sqlalchemy import insert, update
from sqlalchemy.exc import ProgrammingError

from etl.models.omopcdm54 import CDMSummary
from etl.util.db import AbstractSession, FakeSession
from etl.util.memory import get_memory_use


def with_log_to_summary_table(func) -> types.FunctionType:
    def wrapper(obj, *args, **kwargs):
        start_datetime = datetime.now()
        result = func(obj, *args, **kwargs)
        if not isinstance(obj.session, FakeSession):
            try:
                log_transform_to_summary_table(
                    obj.session,
                    transform_name=obj.key,
                    start_transform_datetime=start_datetime,
                    end_transform_datetime=datetime.now(),
                    memory_used=get_memory_use(),
                )
            except ProgrammingError as e:
                if "cdm_summary does not exist" not in str(e):
                    raise e
        return result

    return wrapper


def log_transform_to_summary_table(
    session: AbstractSession,
    transform_name: str,
    **kwargs,
) -> None:

    transforms_already_logged = {
        el[0] for el in session.query(CDMSummary.transform_name).all()
    }
    if transform_name in transforms_already_logged:
        session.execute(
            update(CDMSummary)
            .where(CDMSummary.transform_name == transform_name)
            .values(**kwargs)
        )
    else:
        session.execute(
            insert(CDMSummary).values(transform_name=transform_name, **kwargs)
        )
