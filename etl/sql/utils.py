""" Utility functions for transform queries """

from typing import List, Union

from sqlalchemy import (
    DATE,
    case,
    cast,
    column,
    func,
    literal,
    select,
    union_all,
)
from sqlalchemy.dialects.postgresql import INTERVAL
from sqlalchemy.orm import DeclarativeMeta
from sqlalchemy.sql.expression import CTE
from sqlalchemy.sql.selectable import Select


def get_column(table: Union[CTE, DeclarativeMeta], column_name: str):
    """
    Helper function to allow both normal ORM tables and CTE tables in
    get_era_select below
    """
    try:
        return getattr(table.c, column_name)
    except AttributeError:
        return getattr(table, column_name)


def get_era_select(
    clinical_table: Union[CTE, DeclarativeMeta],
    key_columns: List[str] = None,
    start_column: str = None,
    end_column: str = None,
) -> Select:

    if not key_columns:
        raise NotImplementedError(
            "derive_eras expected at least one grouping column"
        )

    if isinstance(key_columns, str):
        key_columns = [key_columns]

    original_lookback_interval = cast(
        get_column(clinical_table, "era_lookback_interval"), INTERVAL
    )

    combined = union_all(
        select(
            *[get_column(clinical_table, g) for g in key_columns],
            original_lookback_interval.label("lookback_interval"),
            get_column(clinical_table, start_column).label("a"),
            literal(1).label("d"),
            literal(1).label("n"),
        ).where(original_lookback_interval.isnot(None)),
        select(
            *[get_column(clinical_table, g) for g in key_columns],
            original_lookback_interval.label("lookback_interval"),
            (
                get_column(clinical_table, end_column)
                + original_lookback_interval
            ).label("a"),
            literal(-1).label("d"),
            literal(0).label("n"),
        ).where(original_lookback_interval.isnot(None)),
    )

    weighted_endpoints = select(
        *[column(g) for g in key_columns],
        combined.c.lookback_interval,
        combined.c.a,
        func.sum(combined.c.d).label("d"),
        func.sum(combined.c.n).label("n"),
    ).group_by(
        *[column(g) for g in key_columns],
        combined.c.lookback_interval,
        combined.c.a,
    )

    endpoints_with_coverage = select(
        *weighted_endpoints.columns,
        (
            func.sum(weighted_endpoints.c.d).over(
                order_by=[column(g) for g in [*key_columns, "a"]]
            )
            - weighted_endpoints.c.d
        ).label("c"),
    )

    equivalence_classes = select(
        *endpoints_with_coverage.columns,
        func.count(case((endpoints_with_coverage.c.c == 0, 1)))
        .over(order_by=[column(g) for g in [*key_columns, "a"]])
        .label("id"),
    )

    eras = select(
        *[getattr(equivalence_classes.c, g) for g in key_columns],
        cast(func.min(equivalence_classes.c.a), DATE).label("era_start"),
        cast(
            func.max(
                equivalence_classes.c.a
                - equivalence_classes.c.lookback_interval
            ),
            DATE,
        ).label("era_end"),
        func.sum(equivalence_classes.c.n).label("era_count"),
    ).group_by(
        *[getattr(equivalence_classes.c, g) for g in key_columns],
        equivalence_classes.c.id,
    )

    # We need this final step to get eras in date and not timestamps
    return select(
        *[getattr(eras.c, g) for g in key_columns],
        eras.c.era_start,
        eras.c.era_end,
        func.sum(eras.c.era_count).label("era_count"),
    ).group_by(
        *[getattr(eras.c, g) for g in key_columns],
        eras.c.era_start,
        eras.c.era_end,
    )
