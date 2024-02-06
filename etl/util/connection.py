"""helpers and utilites for connection details"""
from typing import Dict, Final, List, NamedTuple, Optional

POSTGRES_DB: Final[str] = "postgresql"
SUPPORTED_DBMS: Final[List[str]] = [POSTGRES_DB]

DEFAULT_DB: Final[str] = POSTGRES_DB
DEFAULT_PORT: Final[int] = 5432


class ConnectionDetails(NamedTuple):
    """A simple type for storing connection details"""

    host: str
    dbms: Optional[str] = DEFAULT_DB
    dbname: Optional[str] = ""
    port: Optional[int] = DEFAULT_PORT
    user: Optional[str] = ""
    password: Optional[str] = ""
    schema: Optional[str] = ""


def get_connection_details(config: Dict) -> ConnectionDetails:
    """
    Does not read from a file, expects a in-memory python dictionary object
    to make a corresponding ConnectionDetails namedtuple.

    Rationale is that it is easier to test and isolates logic of config mapping.
    """
    ports = {
        "oracle": 1521,
        "postgresql": 5432,
        "redshift": 5439,
        "netezza": 5480,
    }

    dbms = config["dbms"]
    port = config["port"] if "port" in config else ports.get(dbms, None)
    schema = config["schema"] if "schema" in config else ""

    return ConnectionDetails(
        dbms=dbms,
        host=config["server"],
        dbname=config["dbname"],
        port=port,
        schema=schema,
        user=config["user"],
        password=config["password"],
    )
