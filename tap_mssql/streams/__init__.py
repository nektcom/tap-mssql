"""Stream classes for tap-mssql."""

from tap_mssql.streams.common import MSSQLStream
from tap_mssql.streams.query import MSSQLQueryStream

__all__ = ["MSSQLQueryStream", "MSSQLStream"]
