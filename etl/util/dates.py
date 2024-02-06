"""Date related helpers"""
import datetime

import pandas as pd


def format_date(date: str, default: str = None) -> str:
    """
    Transforms the dates to proper values.
    Strips the dates from trailing dashes and adds missing month and day
    """
    formatted_date = None
    if not date:
        formatted_date = pd.to_datetime(default, errors="coerce")
    date = str(date).strip("-")
    if len(date) == 4:
        formatted_date = pd.to_datetime(f"{date}-01-01", errors="coerce")
    if len(date) == 7:
        formatted_date = pd.to_datetime(f"{date}-01", errors="coerce")
    if len(date) == 10:
        formatted_date = pd.to_datetime(date, errors="coerce")
    if default is not None and (
        formatted_date is pd.NaT or formatted_date is None
    ):
        formatted_date = pd.to_datetime(default, errors="coerce")
    return formatted_date


def todays_date(include_time=False) -> str:
    """
    datetime now in format '%Y-%m-%d %H:%M:%S'
    """
    if include_time:
        return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return datetime.datetime.now().strftime("%Y-%m-%d")
