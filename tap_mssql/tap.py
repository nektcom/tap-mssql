"""MSSQL tap class."""

from __future__ import annotations

import copy
import sys
from functools import cached_property
from typing import TYPE_CHECKING, Any, cast

from nekt_singer_sdk import SQLStream, SQLTap, Stream
from nekt_singer_sdk import typing as th  # JSON schema typing helpers
from nekt_singer_sdk.contrib.msgspec import MsgSpecWriter
from nekt_singer_sdk.singerlib import Catalog, Metadata, Schema, StateMessage
from sqlalchemy.engine import URL
from sqlalchemy.engine.url import make_url

from tap_mssql.connector import MSSQLConnector
from tap_mssql.streams import MSSQLStream

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence


class TapMSSQL(SQLTap):
    name = "tap-mssql"
    default_stream_class = MSSQLStream
    message_writer_class = MsgSpecWriter

    def __init__(
        self,
        *args: tuple,
        **kwargs: dict,
    ) -> None:
        """Construct a MSSQL tap.

        Should use JSON Schema instead
        See https://github.com/meltano/sdk/pull/1525
        """
        super().__init__(*args, **kwargs)
        sql_alchemy_url_exists = self.config.get("sqlalchemy_url") is not None
        individual_url_params_exist = all(
            [
                self.config.get("host") is not None,
                self.config.get("database") is not None,
            ]
        )
        if not (sql_alchemy_url_exists or individual_url_params_exist):
            msg = "Need either the sqlalchemy_url to be set or host and database to be set"
            self.user_logger.error(msg)
            sys.exit(1)

    config_jsonschema = th.PropertiesList(
        th.Property(
            "host",
            th.StringType,
            description=("Hostname for MSSQL instance. Note if sqlalchemy_url is set this will be ignored."),
        ),
        th.Property(
            "port",
            th.IntegerType,
            default=1433,
            description=("The port on which MSSQL is awaiting connection. Note if sqlalchemy_url is set this will be ignored."),
        ),
        th.Property(
            "user",
            th.StringType,
            description=("User name used to authenticate. Note if sqlalchemy_url is set this will be ignored."),
        ),
        th.Property(
            "password",
            th.StringType,
            secret=True,
            description=("Password used to authenticate. Note if sqlalchemy_url is set this will be ignored."),
        ),
        th.Property(
            "database",
            th.StringType,
            description=("Database name. Note if sqlalchemy_url is set this will be ignored."),
        ),
        th.Property(
            "streams_in_parallel",
            th.IntegerType,
            default=1,
            description="Optional. Maximum number of streams in parallel.",
        ),
        th.Property(
            "sqlalchemy_url",
            th.StringType,
            secret=True,
            description=(
                "Example mssql+pymssql://[username]:[password]@localhost:1433/[db_name][?options] "
                "see https://docs.sqlalchemy.org/en/20/dialects/mssql.html for more information"
            ),
        ),
        th.Property(
            "filter_dbs",
            th.StringType,
            description=(
                "Comma-separated list of schema names to filter. If provided, the tap will only process "
                "the specified MSSQL schemas and ignore others. If left blank, the tap automatically "
                "determines all available MSSQL schemas (excluding system schemas)."
            ),
        ),
        th.Property(
            "characterset",
            th.StringType,
            default="utf8",
            description="Character set for the connection. Note if sqlalchemy_url is set this will be ignored.",
        ),
        th.Property(
            "tds_version",
            th.StringType,
            default="7.3",
            description="TDS protocol version. Note if sqlalchemy_url is set this will be ignored.",
        ),
        th.Property(
            "use_date_datatype",
            th.BooleanType,
            default=False,
            description=(
                "If true, date columns will use date format instead of datetime. "
                "Time columns will use time format instead of datetime."
            ),
        ),
        th.Property(
            "use_singer_decimal",
            th.BooleanType,
            default=False,
            description=(
                "If true, decimal/numeric columns will use singer.decimal format as strings "
                "to preserve precision for large/precise numbers."
            ),
        ),
        th.Property(
            "cursor_array_size",
            th.IntegerType,
            default=1,
            description="Number of rows to fetch at a time from the database cursor.",
        ),
        th.Property(
            "default_replication_method",
            th.StringType,
            description="Default replication method to use when not specified in catalog (FULL_TABLE, INCREMENTAL, or LOG_BASED).",
        ),
        th.Property(
            "conn_properties",
            th.StringType,
            description="Additional connection properties for specific version settings.",
        ),
        th.Property(
            "enable_tds_logging",
            th.BooleanType,
            default=False,
            description="Enable TDS protocol logging for debugging purposes.",
        ),
    ).to_dict()

    def get_sqlalchemy_url(self, config: Mapping[str, Any]) -> str:
        """Generate a SQLAlchemy URL.

        Args:
            config: The configuration for the connector.
        """
        if config.get("sqlalchemy_url"):
            return cast(str, config["sqlalchemy_url"])

        sqlalchemy_url = URL.create(
            drivername="mssql+pymssql",
            username=config.get("user"),
            password=config.get("password"),
            host=config["host"],
            port=config.get("port", 1433),
            database=config["database"],
            query=self.get_sqlalchemy_query(config=config),
        )
        return cast(str, sqlalchemy_url)

    def get_sqlalchemy_query(self, config: Mapping[str, Any]) -> dict:
        """Get query parameters for SQLAlchemy URL.

        Note: For pymssql, charset and tds_version should be passed as connect_args,
        not as URL query parameters.
        """
        query = {}
        return query

    @cached_property
    def connector(self) -> MSSQLConnector:
        url = make_url(self.get_sqlalchemy_url(config=self.config))

        return MSSQLConnector(
            is_running_discovery=self.is_running_discovery,
            config=dict(self.config),
            sqlalchemy_url=url.render_as_string(hide_password=False),
        )

    @property
    def catalog_dict(self) -> dict:
        if self._catalog_dict:
            return self._catalog_dict

        if self.input_catalog:
            return self.input_catalog.to_dict()

        result: dict[str, list[dict]] = {"streams": []}
        result["streams"].extend(self.connector.discover_catalog_entries())

        self._catalog_dict: dict = result
        return self._catalog_dict

    @cached_property
    def catalog(self) -> Catalog:
        """Get the tap's working catalog.

        Override to do LOG_BASED modifications.

        Returns:
            A Singer catalog object.
        """
        new_catalog: Catalog = Catalog()
        modified_streams: list = []
        for stream in super().catalog.streams:
            stream_modified = False
            new_stream = copy.deepcopy(stream)

            # If LOG_BASED, apply existing nullability and _sdc column logic
            if new_stream.replication_method == "LOG_BASED" and new_stream.schema.properties:
                for property in new_stream.schema.properties.values():
                    if "null" not in property.type:
                        if isinstance(property.type, list):
                            property.type.append("null")
                        else:
                            property.type = [property.type, "null"]
                if new_stream.schema.required:
                    stream_modified = True
                    new_stream.schema.required = None
                if "_sdc_deleted_at" not in new_stream.schema.properties:
                    stream_modified = True
                    new_stream.schema.properties.update({"_sdc_deleted_at": Schema(type=["string", "null"], format="date-time")})
                    new_stream.metadata.update({("properties", "_sdc_deleted_at"): Metadata(Metadata.InclusionType.AVAILABLE, True, None)})
                if "_sdc_lsn" not in new_stream.schema.properties:
                    stream_modified = True
                    new_stream.schema.properties.update({"_sdc_lsn": Schema(type=["string", "null"])})
                    new_stream.metadata.update({("properties", "_sdc_lsn"): Metadata(Metadata.InclusionType.AVAILABLE, True, None)})
            if stream_modified:
                modified_streams.append(new_stream.tap_stream_id)
            new_catalog.add_stream(new_stream)
        if modified_streams:
            self.internal_logger.info(
                "One or more LOG_BASED catalog entries were modified "
                f"({modified_streams=}) to allow nullability and include _sdc columns. "
                "See README for further information."
            )
        return new_catalog

    @property
    def streams(self) -> dict[str, Stream]:
        if self._streams is None:
            self._streams = {}

            for stream in self.load_streams():
                if self.catalog is not None:
                    stream.apply_catalog(self.catalog)
                self._streams[stream.name] = stream
        return self._streams

    def discover_streams(self) -> Sequence[Stream]:
        streams: list[SQLStream] = []
        for catalog_entry in self.catalog_dict["streams"]:
            streams.append(MSSQLStream(self, catalog_entry, connector=self.connector))
        return streams

    def sync_all(self) -> None:
        """Sync all streams."""
        self._reset_state_progress_markers()
        self._set_compatible_replication_methods()
        if self.state:
            self.write_message(StateMessage(value=self.state))

        other_streams = [stream for stream in self.streams.values() if stream.selected]

        for stream in other_streams:
            if not stream.selected and not stream.has_selected_descendents:
                self.logger.info("Skipping deselected stream '%s'.", stream.name)
                continue

            if stream.parent_stream_type:
                self.logger.debug(
                    "Child stream '%s' is expected to be called by parent stream '%s'. Skipping direct invocation.",
                    type(stream).__name__,
                    stream.parent_stream_type.__name__,
                )
                continue

            stream.sync()
            stream.finalize_state_progress_markers()

        # this second loop is needed for all streams to print out their costs
        # including child streams which are otherwise skipped in the loop above
        for stream in self.streams.values():
            stream.log_sync_costs()


if __name__ == "__main__":
    TapMSSQL.cli()
