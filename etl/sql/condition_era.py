"Condition era logic."

from datetime import datetime

from sqlalchemy import and_, case, cast, func, insert, or_, literal
from sqlalchemy.dialects.postgresql import INTERVAL
from sqlalchemy.sql import Insert

from ..models.omopcdm54.clinical import Stem as OmopStem
from ..models.omopcdm54.standardized_derived_elements import (
    ConditionEra as OmopConditionEra,
)
from ..sql.merge.mergeutils import _unite_intervals_sql
from ..util.db import (
    AbstractSession,
    get_environment_variable as get_era_lookback_interval,
)

DEFAULT_ERA_LOOKBACK_INTERVAL = get_era_lookback_interval(
    "CONDITION_ERA_LOOKBACK", "31 days"
)


def get_condition_era_insert(session: AbstractSession = None) -> Insert:
    lookback_interval = cast(
        func.coalesce(
            OmopStem.era_lookback_interval, DEFAULT_ERA_LOOKBACK_INTERVAL
        ),
        INTERVAL,
    )

    CtePreviousConditionDate = (
        session.query(
            OmopStem.person_id,
            OmopStem.concept_id.label("condition_concept_id"),
            (
                func.coalesce(OmopStem.start_date, OmopStem.end_date)
                - lookback_interval
            ).label("condition_era_start_date"),
            case(
                (OmopStem.end_date > datetime.now(), OmopStem.start_date),
                else_=func.coalesce(OmopStem.end_date, OmopStem.start_date),
            ).label("condition_era_end_date"),
            literal(1).label("condition_occurrence_count")
        )
        .where(
            and_(
                OmopStem.concept_id != 0,
                OmopStem.domain_id == "Condition",
                OmopStem.start_date.isnot(None),
                OmopStem.end_date.isnot(None),
                OmopStem.start_date <= OmopStem.end_date,
            )
        )
        .order_by(
            OmopStem.person_id,
            OmopStem.concept_id,
            OmopStem.start_date,
            OmopStem.end_date,
        )
        .cte(name="cte_previous_condition_date")
    )

    sql = _unite_intervals_sql(
        source_cdm_table=CtePreviousConditionDate,
        target_cdm_table=OmopConditionEra,
        key_columns=["person_id", "condition_concept_id"],
        interval_start_column="condition_era_start_date",
        interval_end_column="condition_era_end_date",
        agg_columns="condition_occurrence_count",
        agg_function="SUM",
    )
    import pdb;pdb.set_trace()
    return sql
