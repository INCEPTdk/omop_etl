"""
Preprocessing the source data
"""
import logging
from typing import Dict

import numpy as np
import pandas as pd

logger = logging.getLogger("ETL.Core.PreProcessing")


def all_object_columns_lower_case(input_df: pd.DataFrame) -> pd.DataFrame:
    for column in input_df.select_dtypes(
        include=object, exclude=["datetime", "timedelta"]
    ):
        try:
            input_df[column] = input_df[column].str.lower()
        # skip non confirming object fields
        except AttributeError:
            continue
    return input_df


def replace_to_nan(input_df: pd.DataFrame) -> pd.DataFrame:
    for column in input_df:
        input_df[column].replace(
            ["nan", "none", "<not performed>"], np.nan, inplace=True
        )
    return input_df


def set_columns_to_lowercase(input_df: pd.DataFrame) -> pd.DataFrame:
    input_df.columns = map(str.lower, input_df.columns)
    return input_df


# pylint: disable=unused-argument
def log_missing_columns(tablename: str, input_df: pd.DataFrame) -> None:
    # TO-DO: implement
    pass


def preprocessing_transform(
    data: Dict[str, pd.DataFrame]
) -> Dict[str, pd.DataFrame]:
    for key, value in data.items():
        data[key] = all_object_columns_lower_case(value)
        data[key] = replace_to_nan(value)
        data[key] = set_columns_to_lowercase(value)
        log_missing_columns(key, value)
    return data
