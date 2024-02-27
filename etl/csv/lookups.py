"""csv files and lookups"""
from typing import Any, Dict, Final, Optional

import pandas as pd
from sqlalchemy import case
from sqlalchemy.sql.elements import Case

from etl.models.tempmodels import ConceptLookup

SHAK_LOOKUP_DF: Final[pd.DataFrame] = pd.read_csv(
    "etl/csv/shak_lookup.csv", sep="\t", encoding="iso-8859-1"
)

CONCEPT_LOOKUP_DF: Final[pd.DataFrame] = pd.read_csv(
    "etl/csv/concept_lookup.csv", sep=";"
)


def get_concept_lookup_dict(filter_: str) -> Dict:
    fdf = CONCEPT_LOOKUP_DF[
        CONCEPT_LOOKUP_DF[ConceptLookup.filter.key] == filter_
    ]
    return dict(
        zip(
            fdf[ConceptLookup.concept_string.key],
            fdf[ConceptLookup.concept_id.key],
        )
    )


def generate_lookup_case(
    lookup_dict: Dict[str, Any], source_col, default: Optional[Any] = None
) -> Case:
    return case(
        [(source_col == k, v) for k, v in lookup_dict.items()], else_=default
    )
