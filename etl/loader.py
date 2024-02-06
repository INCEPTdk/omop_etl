"""Load source files into memory"""
import logging
import os
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd

from .util.exceptions import ETLFatalErrorException

logger = logging.getLogger("ETL.Core")


class Loader:
    """An empty loader to load in csv files"""

    __slots__ = ["_data"]

    def __init__(self) -> None:
        """Initialise it with some data"""
        self._data = {}

    def load(self) -> "Loader":
        """Does nothing"""
        return self

    def reset(self) -> None:
        """Reset the loaded data"""
        self._data = {}

    def _update(self, key: str, value: Any) -> None:
        self._data[key] = value

    def get(self, key: str) -> Any:
        """Get a loaded entry by key"""
        return self._data.get(key)

    @property
    def data(self) -> Dict[str, Any]:
        return self._data


EmptyLoader = Loader


class CSVFileLoader(Loader):
    """A source data loader for CSV inputs"""

    def __init__(
        self,
        directory: Path,
        models: List[Any],
        delimiter: str = ",",
        extension: str = ".csv",
    ) -> None:
        super().__init__()
        self.models = models
        self.directory = directory
        self.delimiter = delimiter
        self.extension = extension
        self.encoding = "utf-8"

    def load(self) -> Loader:
        """Load from source csv files"""
        self.reset()
        for model in self.models:
            tablename = model.__tablename__
            input_file = self.directory / f"{tablename}{self.extension}"
            if not os.path.exists(input_file):
                logger.error(
                    "The following table is expected but is missing: %s, please check input data",
                    tablename,
                )
                raise ETLFatalErrorException(
                    f"Table: {tablename} missing. Expected file name: {input_file}."
                )

            self._update(
                tablename,
                pd.read_csv(
                    input_file,
                    sep=self.delimiter,
                    encoding=self.encoding,
                    low_memory=False,
                ),
            )

        return self
