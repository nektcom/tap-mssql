"""Stream class for query-based MSSQL streams."""

from __future__ import annotations

import sys
from typing import TYPE_CHECKING, Any

import sqlalchemy as sa
from nekt_singer_sdk import SQLStream
from nekt_singer_sdk.custom_logger import user_logger
from nekt_singer_sdk.helpers._typing import TypeConformanceLevel

from tap_mssql.connector import MSSQLConnector

if TYPE_CHECKING:
    from collections.abc import Iterable


class MSSQLQueryStream(SQLStream):
    """Stream class for query-based MSSQL streams."""

    connector_class = MSSQLConnector
    TYPE_CONFORMANCE_LEVEL = TypeConformanceLevel.ROOT_ONLY
    is_sorted = False

    def __init__(
        self,
        tap,  # noqa: ANN001
        catalog_entry: dict,
        *,
        connector: MSSQLConnector,
        query: str,
    ) -> None:
        super().__init__(tap, catalog_entry, connector=connector)
        self._query = query

    def get_records(self, context: dict | None) -> Iterable[dict[str, Any]]:
        if context:
            msg = f"Stream '{self.name}' does not support partitioning."
            self._tap.user_logger.error(msg)
            sys.exit(1)

        selected_column_names = list(self.get_selected_schema()["properties"])
        raw_query = sa.text(self._query)

        with self.connector._connect() as conn:  # noqa: SLF001
            user_logger.info(f"Getting records for query stream: '{self.name}'")

            result = conn.execution_options(stream_results=True).execute(raw_query)
            if self.config.get("cursor_array_size", 1) > 1:
                result = result.yield_per(self.config["cursor_array_size"])

            for record in result.mappings():
                row = dict(record)
                # Filter to only selected columns
                if selected_column_names:
                    row = {k: v for k, v in row.items() if k in selected_column_names}
                transformed_record = self.post_process(row)
                if transformed_record is None:
                    continue
                yield transformed_record
