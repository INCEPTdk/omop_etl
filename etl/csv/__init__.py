"""csv files and lookups"""
from typing import Final

import pandas as pd

LOOKUP_DF: Final[pd.DataFrame] = pd.read_csv(
    "etl/csv/shak_lookup.tsv", sep="\t", dtype=str
)
