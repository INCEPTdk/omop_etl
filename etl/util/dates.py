"""Date related helpers"""

import datetime


def todays_date(include_time=False) -> str:
    """
    datetime now in format '%Y-%m-%d %H:%M:%S'
    """
    if include_time:
        return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return datetime.datetime.now().strftime("%Y-%m-%d")
