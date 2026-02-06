"""SQL client handling."""

from __future__ import annotations

import sys
from datetime import datetime
from typing import TYPE_CHECKING, Any

from nekt_singer_sdk import SQLStream
from nekt_singer_sdk.custom_logger import user_logger
from nekt_singer_sdk.helpers._typing import TypeConformanceLevel

from tap_mssql.connector import MSSQLConnector

if TYPE_CHECKING:
    from collections.abc import Iterable


class MSSQLStream(SQLStream):
    """Stream class for MSSQL streams."""

    connector_class = MSSQLConnector

    # Objects won't be selected without type_confomance_level to ROOT_ONLY
    TYPE_CONFORMANCE_LEVEL = TypeConformanceLevel.ROOT_ONLY

    is_sorted = False

    @staticmethod
    def _parse_replication_key_value(value: str) -> str | datetime:
        """Parse a replication key value, converting datetime strings to datetime objects.

        pymssql cannot send ISO 8601 datetime strings (e.g. '2025-09-01T17:49:59+00:00')
        to SQL Server as parameters. Converting to datetime objects allows pymssql to
        handle the formatting correctly.
        """
        if not isinstance(value, str):
            return value
        for fmt in (
            "%Y-%m-%dT%H:%M:%S.%f%z",
            "%Y-%m-%dT%H:%M:%S%z",
            "%Y-%m-%dT%H:%M:%S.%f",
            "%Y-%m-%dT%H:%M:%S",
        ):
            try:
                return datetime.strptime(value, fmt)
            except ValueError:
                continue
        return value

    def get_records(self, context: dict | None) -> Iterable[dict[str, Any]]:
        if context:
            msg = f"Stream '{self.name}' does not support partitioning."
            self._tap.user_logger.error(msg)
            sys.exit(1)

        # pulling rows with only selected columns from stream
        selected_column_names = list(self.get_selected_schema()["properties"])
        table = self.connector.get_table(
            self.fully_qualified_name,
            column_names=selected_column_names,
        )

        query = table.select()
        if self.replication_key:
            replication_key_col = table.columns[self.replication_key]
            query = query.order_by(replication_key_col)

            start_val = self.get_starting_replication_key_value(context)
            if start_val:
                start_val = self._parse_replication_key_value(start_val)
                query = query.where(replication_key_col >= start_val)

        # Use standard streaming approach
        with self.connector._connect() as conn:  # noqa: SLF001
            user_logger.info(f"Getting records for query: '{query}'")

            result = conn.execution_options(stream_results=True).execute(query)
            if self.config.get("cursor_array_size", 1) > 1:
                result = result.yield_per(self.config["cursor_array_size"])

            for record in result.mappings():
                # TODO: Standardize record mapping type
                # https://github.com/meltano/sdk/issues/2096
                transformed_record = self.post_process(dict(record))
                if transformed_record is None:
                    # Record filtered out during post_process()
                    continue
                yield transformed_record
