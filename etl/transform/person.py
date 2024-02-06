"""
Person transformation
"""
import logging
from typing import Final

import pandas as pd

from ..models.omopcdm54 import Person as TARGET
from .base_operation import BaseOperation

DEFAULT_YEAR_OF_BIRTH: Final[int] = 1800

logger = logging.getLogger("ETL.Person")


class PersonOperation(BaseOperation):
    """Person transformation operation"""

    def __init__(
        self,
    ) -> None:
        super().__init__(
            key=str(TARGET.__table__),
            description="Person Transformation",
        )

    def _run(self, *args, **kwargs) -> pd.DataFrame:
        # TO-DO: implement
        input_df = pd.DataFrame()

        if input_df is None:
            return pd.DataFrame()

        output_df = pd.DataFrame()

        return output_df
