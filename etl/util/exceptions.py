"""Module for ETL specific exceptions"""


class ETLException(Exception):
    """Base exception"""


class EmptyJSONFileException(ETLException):
    """Use if JSON file is empty"""


class DBConnectionException(ETLException):
    """Use if a problem with the database"""


class FrozenClassException(ETLException):
    """Use for frozen classes i.e. slots"""


class DependencyNotFoundException(ETLException):
    """Throw if library or tool not installed"""


class TransformationErrorException(ETLException):
    """Throw when performing a transformation"""


class ETLFatalErrorException(ETLException):
    """Throw when ETL fails"""

class InvalidEraLookbackInterval(ETLException):
    """Throw when era lookback interval is invalid"""