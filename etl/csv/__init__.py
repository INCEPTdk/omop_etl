"""csv files and lookups"""
from typing import Final

import pandas as pd

rows = pd.read_csv("etl/csv/shak_lookup.tsv", sep="\t", dtype=str).itertuples(
    index=False, name="CareSite"
)

SHAK_LOOKUP: Final[dict] = {r.department_shak_code: r._asdict() for r in rows}
