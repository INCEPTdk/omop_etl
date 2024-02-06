"""Utilies for files"""
import json
from typing import Dict

from .exceptions import EmptyJSONFileException


def load_config_from_file(filename: str, encoding="utf-8") -> Dict:
    """Reads a JSON file and returns a dictionary of the JSON contents."""
    details = {}
    with open(filename, "rt", encoding=encoding) as fconfig:
        details = json.load(fconfig)

    if not details:
        raise EmptyJSONFileException(
            f"No connection details found in {filename}"
        )

    return details
