""" A module for generating random values, dates, etc"""
# pylint: disable=invalid-name
import datetime
from random import randint, random
from typing import Any, Dict, Optional

from faker import Faker
from sqlalchemy import Column
from sqlalchemy.sql.sqltypes import Date, DateTime, Float, Integer, String

from ..util.exceptions import ETLException
from ..util.uuid import generate_uuid_as_str

_fake = Faker()


def static_vars(**kwargs):
    """Static variable decorator"""

    def decorate(func):
        for k, v in kwargs.items():
            setattr(func, k, v)
        return func

    return decorate


@static_vars(count=0)
def generate_int_primary_key() -> int:
    """Generate a primary key"""
    # pylint: disable=E1101
    generate_int_primary_key.count += 1
    # pylint: disable=E1101
    return generate_int_primary_key.count


def generate_varchar_primary_key(max_length: Optional[int] = 32) -> str:
    """Generate a primary key"""
    return generate_uuid_as_str().replace("-", "")[:max_length]


def generate_random_date(
    first_year: int = 1800, last_year: int = 2021
) -> datetime.date:
    """Generate a random date"""
    year = randint(first_year, last_year)
    month = randint(1, 12)
    if month in [4, 6, 9, 11]:
        return datetime.date(year, month, randint(1, 30))
    if month in [1, 3, 5, 7, 8, 10, 12]:
        return datetime.date(year, month, randint(1, 31))
    return datetime.date(year, month, randint(1, 28))


def generate_random_datetime(
    first_year: int = 1800, last_year: int = 2021
) -> datetime.date:
    """Generate a random datetime"""
    d = generate_random_date(first_year, last_year)
    hour = randint(0, 23)
    minute = randint(0, 59)
    second = randint(0, 59)
    return datetime.datetime(d.year, d.month, d.day, hour, minute, second)


def generate_random_int(vmin: int = 0, vmax: int = 10_000) -> int:
    """Generate a random integer"""
    return randint(vmin, vmax)


def generate_random_float(vmax: float = 1e6) -> int:
    """Generate a random float"""
    sign = (
        1,
        -1,
    )[randint(0, 1)]
    return sign * random() * vmax


def generate_random_str(
    str_type: str = "text", max_length: Optional[int] = None
) -> int:
    """Generate a random str"""
    string_types = ["name", "address", "text"]
    if str_type not in string_types:
        raise ETLException(f"String must be one of: {string_types}")

    s = getattr(_fake, str_type)()
    if max_length is not None:
        return s[:max_length]
    return s


def _is_null() -> bool:
    return bool(randint(0, 1))


def _int_col(x: Column, config: Optional[Dict] = None):
    if config is None:
        config = {}
    if x.primary_key:
        return generate_int_primary_key()
    if x.nullable:
        if _is_null():
            return None

    if x.key in config:
        return generate_random_int(
            vmin=config[x.key]["min"], vmax=config[x.key]["max"]
        )

    return generate_random_int()


def _float_col(x: Column):
    if x.nullable:
        if _is_null():
            return None
    return generate_random_float()


def _string_col(x: Column):
    if x.primary_key:
        return generate_varchar_primary_key(max_length=x.type.length)
    if x.nullable:
        if _is_null():
            return None
    return generate_random_str("text", max_length=x.type.length)


def _date_col(x: Column):
    if x.nullable:
        if _is_null():
            return None
    return generate_random_date()


def _datetime_col(x: Column):
    if x.nullable:
        if _is_null():
            return None
    return generate_random_datetime()


def generate_dummy_data(
    model: Any, model_config: Optional[Dict] = None
) -> Dict[str, Any]:
    """
    Poor mans approach to generate random table entries based on a given model

    to-do: support foriegn keys
    """

    if model_config is None:
        model_config = {}

    columns = model.__table__.columns.values()
    mappings = {
        Integer: lambda x: _int_col(x, model_config),
        Float: _float_col,
        String: _string_col,
        Date: _date_col,
        DateTime: _datetime_col,
    }
    data = {c.key: mappings[type(c.type)](c) for c in columns}
    return data
