"""All common functions and definitions used in SQL statements here"""
from datetime import date, datetime
from typing import Dict, Final, Optional, Union

from .util.sql import clean_sql

CONCEPT_ID_MALE: Final[int] = 8507
CONCEPT_ID_FEMALE: Final[int] = 8532
CONCEPT_ID_REGISTRY: Final[int] = 32879
CONCEPT_ID_COMPLICATIONS: Final[int] = 3009166
CONCEPT_ID_CAUSE_OF_DEATH: Final[int] = 4083743
CONCEPT_ID_OBSERVATION_PERIOD: Final[int] = 44814724
CONCEPT_ID_BONE_MARROW_TRANSPLANT: Final[int] = 4028623
CONCEPT_ID_EHR: Final[int] = 32817
CONCEPT_ID_ABSENCE: Final[int] = 4132135
CONCEPT_ID_PRESENCE: Final[int] = 4181412
CONCEPT_ID_UNKNOWN_ANSWER: Final[int] = 45877986
CONCEPT_ID_KILOGRAM_UNIT: Final[int] = 9529
CONCEPT_ID_PER_LITER_UNIT: Final[int] = 8923
CONCEPT_ID_LITER_UNIT: Final[int] = 8519
CONCEPT_ID_CENTIMETER_UNIT: Final[int] = 8582
CONCEPT_ID_GRAM_PER_LITER_UNIT: Final[int] = 8636
CONCEPT_ID_NOT_KNOWN: Final[int] = 0

DEFAULT_DATE: Final[date] = date(1800, 1, 1)
DEFAULT_DATETIME: Final[datetime] = datetime(
    DEFAULT_DATE.year, DEFAULT_DATE.month, DEFAULT_DATE.day, 0, 0, 0
)


@clean_sql
def if_null_date_sql(
    column_name: str, default_date: date = DEFAULT_DATE
) -> str:
    return f"""
    (
        CASE
           WHEN {column_name} IS NOT NULL THEN {column_name}
           ELSE '{default_date.isoformat()}'
        END
    )
"""


@clean_sql
def concat_source_value_sql(
    source_col: str, max_length: Optional[int] = 250
) -> str:
    return f"""
    (
        CASE
            WHEN {source_col} IS NOT NULL THEN
                SUBSTR(
                    CONCAT_WS(
                        '|',
                        '{source_col}',
                        {source_col}
                    ),
                    1, {max_length}
                )
            ELSE
                SUBSTR(
                    CONCAT_WS(
                        '|',
                        '{source_col}',
                        ''
                    ),
                    1, {max_length}
                )
        END
    )"""


@clean_sql
def case_select_sql(
    col_name: str,
    mappings: Dict[Union[str, int], Union[str, int]],
    default_case: Optional[Union[int, str]] = CONCEPT_ID_NOT_KNOWN,
) -> str:
    case_str = "\n".join(
        [f"WHEN {col_name} = {k} THEN {v}" for k, v in mappings.items()]
    )

    return f"""
    (
        CASE
            {case_str}
            ELSE {default_case}
        END
    )
"""
