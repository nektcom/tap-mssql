"""MSSQL tap class."""

from __future__ import annotations

import atexit
import copy
import io
import signal
import sys
from functools import cached_property
from typing import TYPE_CHECKING, Any, cast

import paramiko
from nekt_singer_sdk import SQLStream, SQLTap, Stream
from nekt_singer_sdk import typing as th  # JSON schema typing helpers
from nekt_singer_sdk.contrib.msgspec import MsgSpecWriter
from nekt_singer_sdk.singerlib import Catalog, Metadata, Schema, StateMessage
from sqlalchemy.engine import URL
from sqlalchemy.engine.url import make_url

from tap_mssql.connector import MSSQLConnector
from tap_mssql.ssh_tunnel import SSHTunnelForwarder
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
            "filter_tables",
            th.StringType,
            description=(
                "Comma-separated list of table names to filter. If provided, the tap will only process "
                "the specified tables and ignore others. Table names should be in format 'schema.table' "
                "or just 'table' (which will match tables in any schema). Supports wildcard patterns "
                "using * (matches any sequence) and ? (matches single character). Examples: 'user_*', "
                "'dbo.order_*', 'temp_????'. If left blank, all tables in the selected schemas will be processed."
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
        th.Property(
            "encryption",
            th.StringType,
            description=(
                "Encryption setting for the connection. Valid values are 'off', 'request', or 'require'. "
                "Use 'off' to disable encryption, 'request' to prefer encryption but allow unencrypted, "
                "or 'require' to force encrypted connections. Note if sqlalchemy_url is set this will be ignored."
            ),
        ),
        th.Property(
            "ssh_tunnel",
            th.ObjectType(
                th.Property(
                    "enable",
                    th.BooleanType,
                    required=False,
                    default=False,
                    description=("Enable an ssh tunnel (also known as bastion server), see the other ssh_tunnel.* properties for more details"),
                ),
                th.Property(
                    "host",
                    th.StringType,
                    required=False,
                    description="Host of the bastion server, this is the host we'll connect to via ssh",
                ),
                th.Property(
                    "username",
                    th.StringType,
                    required=False,
                    description="Username to connect to bastion server",
                ),
                th.Property(
                    "port",
                    th.IntegerType,
                    required=False,
                    default=22,
                    description="Port to connect to bastion server",
                ),
                th.Property(
                    "password",
                    th.StringType,
                    required=False,
                    secret=True,
                    description="Password for authentication to the bastion server",
                ),
                th.Property(
                    "private_key",
                    th.StringType,
                    required=False,
                    secret=True,
                    description="Private Key for authentication to the bastion server",
                ),
                th.Property(
                    "private_key_password",
                    th.StringType,
                    required=False,
                    secret=True,
                    default=None,
                    description="Private Key Password, leave None if no password is set",
                ),
                th.Property(
                    "run_tunnel_auth_interactive_dumb",
                    th.BooleanType,
                    required=False,
                    default=False,
                    description=("Enable dumb interaction on auth for ssh tunnel"),
                ),
            ),
            required=False,
            description="SSH Tunnel Configuration, this is a json object",
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
        ssh_config = self.config.get("ssh_tunnel", {})

        if ssh_config.get("enable", False):
            # Return a new URL with SSH tunnel parameters
            url = self.ssh_tunnel_connect(ssh_config=ssh_config, url=url)

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

    def guess_key_type(self, key_data: str) -> paramiko.PKey:
        for key_class in (
            paramiko.RSAKey,
            paramiko.DSSKey,
            paramiko.ECDSAKey,
            paramiko.Ed25519Key,
        ):
            try:
                key = key_class.from_private_key(io.StringIO(key_data))  # type: ignore[attr-defined]
            except paramiko.SSHException:  # noqa: PERF203
                continue
            else:
                return key

        errmsg = "Could not determine the key type."
        raise ValueError(errmsg)

    def ssh_tunnel_connect(self, *, ssh_config: dict[str, Any], url: URL) -> URL:
        """Connect to the SSH Tunnel and swap the URL to use the tunnel.

        Args:
            ssh_config: The SSH Tunnel configuration
            url: The original URL to connect to.

        Returns:
            The new URL to connect to, using the tunnel.
        """
        if ssh_config.get("password"):
            credentials = {
                "ssh_password": ssh_config.get("password"),
            }
        else:
            credentials = {
                "ssh_private_key": self.guess_key_type(ssh_config["private_key"]),
                "ssh_private_key_password": ssh_config.get("private_key_password"),
            }

        self.ssh_tunnel: SSHTunnelForwarder = SSHTunnelForwarder(
            ssh_address_or_host=(ssh_config["host"], ssh_config["port"]),
            ssh_username=ssh_config["username"],
            remote_bind_address=(url.host, url.port),
            run_tunnel_auth_interactive_dumb=ssh_config.get("run_tunnel_auth_interactive_dumb", False),
            **credentials,
        )
        self.ssh_tunnel.start()
        self.internal_logger.info("SSH Tunnel started")
        # On program exit clean up, want to also catch signals
        atexit.register(self.clean_up)
        signal.signal(signal.SIGTERM, self.catch_signal)
        # Probably overkill to catch SIGINT, but needed for SIGTERM
        signal.signal(signal.SIGINT, self.catch_signal)

        # Swap the URL to use the tunnel
        return url.set(
            host=self.ssh_tunnel.local_bind_host,
            port=self.ssh_tunnel.local_bind_port,
        )

    def clean_up(self) -> None:
        self.internal_logger.info("Shutting down SSH Tunnel")
        self.ssh_tunnel.stop()

    def catch_signal(self, signum, frame) -> None:  # noqa: ANN001 ARG002
        sys.exit(1)  # Calling this to be sure atexit is called, so clean_up gets called


if __name__ == "__main__":
    TapMSSQL.cli()
