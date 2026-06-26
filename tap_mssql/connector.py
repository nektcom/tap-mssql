"""SQL client handling."""

from __future__ import annotations

import datetime
from typing import TYPE_CHECKING, Any

import singer_sdk.helpers._typing
import sqlalchemy as sa
import sqlalchemy.types
from nekt_singer_sdk import SQLConnector
from nekt_singer_sdk import typing as th
from nekt_singer_sdk.custom_logger import internal_logger
from nekt_singer_sdk.singerlib import CatalogEntry, MetadataMapping, Schema
from sqlalchemy.pool import QueuePool

if TYPE_CHECKING:
    from sqlalchemy.engine import Engine, reflection
    from sqlalchemy.engine.reflection import Inspector

unpatched_conform = singer_sdk.helpers._typing._conform_primitive_property  # noqa: SLF001


def patched_conform(
    elem: Any,  # noqa: ANN401
    property_schema: dict,
) -> Any:  # noqa: ANN401
    """Override type conformance to prevent dates turning into datetimes.

    Ensures that ``date``, ``datetime`` and ``time`` objects are always
    serialised to their ISO formatted string representation so they can be
    safely consumed downstream, regardless of schema settings.
    """

    if isinstance(elem, (datetime.date, datetime.datetime, datetime.time)):
        # ``isoformat()`` gives the canonical representation for all three
        # objects (YYYY-MM-DD, YYYY-MM-DDTHH:MM:SS[.ffffff] and HH:MM:SS[.ffffff])
        return elem.isoformat()

    return unpatched_conform(elem=elem, property_schema=property_schema)


singer_sdk.helpers._typing._conform_primitive_property = patched_conform  # noqa: SLF001


class MSSQLConnector(SQLConnector):
    """Connects to the MSSQL SQL source."""

    def __init__(
        self,
        is_running_discovery: bool,  # noqa: FBT001
        config: dict | None = None,
        sqlalchemy_url: str | None = None,
    ) -> None:
        config = config or {}
        self.pool_size = config.get("streams_in_parallel", 20) * 2
        self.use_date_datatype: bool = config.get("use_date_datatype", False)
        self.use_singer_decimal: bool = config.get("use_singer_decimal", False)
        self.cursor_array_size: int = config.get("cursor_array_size", 1)
        super().__init__(
            is_running_discovery=is_running_discovery,
            config=config,
            sqlalchemy_url=sqlalchemy_url,
        )

    def to_jsonschema_type(
        self,
        sql_type: str | sqlalchemy.types.TypeEngine | type[sqlalchemy.types.TypeEngine] | Any,  # noqa: ANN401
    ) -> dict:
        """Return a JSON Schema representation of the provided type.

        Overridden from SQLConnector to correctly handle MSSQL-specific types.

        By default will call `typing.to_jsonschema_type()` for strings and
        SQLAlchemy types.

        Args:
            sql_type: The string representation of the SQL type, a SQLAlchemy
                TypeEngine class or object, or a custom-specified object.

        Raises:
            ValueError: If the type received could not be translated to
            jsonschema.

        Returns:
            The JSON Schema representation of the provided type.

        """
        type_name = None
        if isinstance(sql_type, str):
            type_name = sql_type
        elif isinstance(sql_type, sqlalchemy.types.TypeEngine):
            type_name = type(sql_type).__name__

        if type_name is not None and type_name in ("JSON",):
            return th.ObjectType().type_dict

        # Use the SDK typing helper to build the base schema.
        result_dict = self.sdk_typing_object(sql_type).type_dict

        return result_dict

    def sdk_typing_object(
        self,
        from_type: str | sqlalchemy.types.TypeEngine | type[sqlalchemy.types.TypeEngine],
    ) -> th.DateTimeType | th.NumberType | th.IntegerType | th.DateType | th.StringType | th.BooleanType | th.TimeType:
        """Return the JSON Schema dict that describes the sql type.

        Args:
            from_type: The SQL type as a string or as a TypeEngine. If a TypeEngine is
                provided, it may be provided as a class or a specific object instance.

        Raises:
            ValueError: If the `from_type` value is not of type `str` or `TypeEngine`.

        Returns:
            A compatible JSON Schema type definition.

        """
        sqltype_lookup: dict[
            str,
            th.DateTimeType | th.NumberType | th.IntegerType | th.DateType | th.StringType | th.BooleanType | th.TimeType,
        ] = {
            # NOTE: This is an ordered mapping, with earlier mappings taking
            # precedence. If the SQL-provided type contains the type name on
            # the left, the mapping will return the respective singer type.
            "datetime2": th.DateTimeType(),
            "datetime": th.DateTimeType(),
            "datetimeoffset": th.DateTimeType(),
            "smalldatetime": th.DateTimeType(),
            "date": th.DateType(),
            "time": th.TimeType(),
            "tinyint": th.IntegerType(),
            "smallint": th.IntegerType(),
            "int": th.IntegerType(),
            "bigint": th.IntegerType(),
            "decimal": th.NumberType(),
            "numeric": th.NumberType(),
            "money": th.NumberType(),
            "smallmoney": th.NumberType(),
            "float": th.NumberType(),
            "real": th.NumberType(),
            "bit": th.BooleanType(),
            "uniqueidentifier": th.StringType(),
            "nvarchar": th.StringType(),
            "nchar": th.StringType(),
            "ntext": th.StringType(),
            "varchar": th.StringType(),
            "char": th.StringType(),
            "text": th.StringType(),
            "string": th.StringType(),
        }
        if isinstance(from_type, str):
            type_name = from_type
        elif isinstance(from_type, sqlalchemy.types.TypeEngine):
            type_name = type(from_type).__name__
        elif isinstance(from_type, type) and issubclass(
            from_type,
            sqlalchemy.types.TypeEngine,
        ):
            type_name = from_type.__name__
        else:
            msg = "Expected `str` or a SQLAlchemy `TypeEngine` object or type."
            raise TypeError(
                msg,
            )

        # Look for the type name within the known SQL type names:
        for sqltype, jsonschema_type in sqltype_lookup.items():
            if sqltype.lower() in type_name.lower():
                # If date/time configuration requires string conversion
                if self.use_date_datatype and isinstance(
                    jsonschema_type,
                    (th.DateType, th.TimeType),
                ):
                    return jsonschema_type

                return jsonschema_type

        return sqltype_lookup["string"]  # safe failover to str

    _EXCLUDE_SCHEMAS = frozenset({
        "information_schema",
        "INFORMATION_SCHEMA",
        "performance_schema",
        "sys",
        "db_accessadmin",
        "db_backupoperator",
        "db_datareader",
        "db_datawriter",
        "db_ddladmin",
        "db_denydatareader",
        "db_denydatawriter",
        "db_owner",
        "db_securityadmin",
        "guest",
    })

    def get_schema_names(
        self,
        engine: Engine | None = None,
        inspected: Inspector | None = None,
        conn: sa.Connection | None = None,
    ) -> list[str]:
        if "filter_dbs" in self.config and self.config["filter_dbs"]:
            return [s.strip() for s in self.config["filter_dbs"].split(",")]

        query = sa.text("SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA ORDER BY SCHEMA_NAME")
        if conn is not None:
            schemas = [row[0] for row in conn.execute(query)]
        else:
            with self._engine.connect() as conn:
                schemas = [row[0] for row in conn.execute(query)]
        return [s for s in schemas if s not in self._EXCLUDE_SCHEMAS]

    def _should_include_table(self, schema_name: str, table_name: str) -> bool:
        """Check if a table should be included based on filter_tables config.

        Supports wildcard patterns:
        - * matches any sequence of characters
        - ? matches any single character

        Args:
            schema_name: The schema name
            table_name: The table name

        Returns:
            True if the table should be included, False otherwise
        """
        import fnmatch

        filter_tables = self.config.get("filter_tables")
        if not filter_tables:
            return True

        table_filters = [t.strip() for t in filter_tables.split(",")]
        full_table_name = f"{schema_name}.{table_name}"

        for table_filter in table_filters:
            # Check for schema.table format with wildcards
            if "." in table_filter:
                if fnmatch.fnmatch(full_table_name, table_filter):
                    return True
            else:
                # Check for table name only match with wildcards
                if fnmatch.fnmatch(table_name, table_filter):
                    return True

        return False

    def _get_filter_names(self, schema_name: str) -> list[str] | None:
        """Get exact table names to pass to SQLAlchemy's filter_names parameter.

        Only returns exact (non-wildcard) table names that match the given schema.
        Returns None if no filter is configured or if only wildcard patterns exist
        (meaning all tables must be fetched for post-filtering).

        Args:
            schema_name: The schema name to filter for.

        Returns:
            A list of table names to filter on, or None if no pre-filtering is possible.
        """
        filter_tables = self.config.get("filter_tables")
        if not filter_tables:
            return None

        table_filters = [t.strip() for t in filter_tables.split(",")]
        has_wildcard = False
        exact_names = []

        for table_filter in table_filters:
            is_wildcard = "*" in table_filter or "?" in table_filter
            if "." in table_filter:
                filter_schema, filter_table = table_filter.split(".", 1)
                if is_wildcard:
                    # Wildcard with schema - only matters if it matches this schema
                    import fnmatch
                    if fnmatch.fnmatch(schema_name, filter_schema):
                        has_wildcard = True
                elif filter_schema == schema_name:
                    exact_names.append(filter_table)
            else:
                if is_wildcard:
                    has_wildcard = True
                else:
                    exact_names.append(table_filter)

        # If there are any wildcard patterns, we can't pre-filter because
        # we'd miss tables that match the wildcard
        if has_wildcard:
            return None

        return exact_names if exact_names else None

    def _fetch_schema_columns_bulk(
        self, schema_name: str, table_type: str, conn: sa.Connection,
    ) -> dict[str, list[tuple]]:
        """Fetch all column metadata for a schema in a single query.

        Returns a dict mapping table_name -> list of column tuples:
        (COLUMN_NAME, DATA_TYPE, IS_NULLABLE, CHARACTER_MAXIMUM_LENGTH,
         NUMERIC_PRECISION, NUMERIC_SCALE)
        """
        query = sa.text(
            "SELECT c.TABLE_NAME, c.COLUMN_NAME, c.DATA_TYPE, c.IS_NULLABLE, "
            "c.CHARACTER_MAXIMUM_LENGTH, c.NUMERIC_PRECISION, c.NUMERIC_SCALE "
            "FROM INFORMATION_SCHEMA.COLUMNS c "
            "JOIN INFORMATION_SCHEMA.TABLES t "
            "  ON c.TABLE_SCHEMA = t.TABLE_SCHEMA AND c.TABLE_NAME = t.TABLE_NAME "
            "WHERE c.TABLE_SCHEMA = :schema AND t.TABLE_TYPE = :table_type "
            "ORDER BY c.TABLE_NAME, c.ORDINAL_POSITION"
        )
        rows = conn.execute(query, {"schema": schema_name, "table_type": table_type})
        columns_by_table: dict[str, list[tuple]] = {}
        for row in rows.fetchall():
            columns_by_table.setdefault(row[0], []).append(row[1:])
        return columns_by_table

    def _fetch_schema_pks_bulk(self, schema_name: str, conn: sa.Connection) -> dict[str, list[str]]:
        """Fetch primary key columns for all tables in a schema in a single query.

        Returns a dict mapping table_name -> list of PK column names (ordered by position).
        """
        query = sa.text(
            "SELECT kcu.TABLE_NAME, kcu.COLUMN_NAME "
            "FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc "
            "JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu "
            "  ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME "
            " AND tc.TABLE_SCHEMA = kcu.TABLE_SCHEMA "
            "WHERE tc.TABLE_SCHEMA = :schema "
            "  AND tc.CONSTRAINT_TYPE = 'PRIMARY KEY' "
            "ORDER BY kcu.TABLE_NAME, kcu.ORDINAL_POSITION"
        )
        rows = conn.execute(query, {"schema": schema_name})
        pks_by_table: dict[str, list[str]] = {}
        for row in rows.fetchall():
            pks_by_table.setdefault(row[0], []).append(row[1])
        return pks_by_table

    def discover_catalog_entries(
        self,
        *,
        exclude_schemas: list[str] | None = None,
        reflect_indices: bool = False,
    ) -> list[dict]:
        """Return a list of catalog entries from discovery.

        Uses bulk INFORMATION_SCHEMA queries instead of SQLAlchemy reflection
        for dramatically better performance on large schemas.
        """
        self.user_discovery_logger.info("Discovering streams...")
        result: list[dict] = []
        exclude_schemas = exclude_schemas or []
        filter_tables_config = self.config.get("filter_tables")

        # Use a single connection for the entire discovery process to avoid
        # connection pool issues on servers with strict connection limits.
        with self._engine.connect() as conn:
            self.user_discovery_logger.info("Discovering schemas...")
            schemas = self.get_schema_names(conn=conn)
            if schemas:
                schema_list = "\n\t- " + "\n\t- ".join(schemas)
                self.user_discovery_logger.info(f"Discovered schemas ({len(schemas)}): {schema_list}")
            else:
                self.user_discovery_logger.error("No schemas discovered, please check your configurations.")

            for schema_name in schemas:
                if schema_name in exclude_schemas:
                    self.user_discovery_logger.info(f"Skipping schema '{schema_name}' (schema in exclude_schemas config).")
                    continue

                # Fetch all PKs for the schema in one query
                self.user_discovery_logger.info(f"Fetching metadata for schema '{schema_name}'...")
                try:
                    schema_pks = self._fetch_schema_pks_bulk(schema_name, conn)
                except Exception as e:
                    self.user_discovery_logger.error(f"Skipping schema '{schema_name}' due to error fetching PKs: {e}")
                    continue

                for table_type, is_view, object_kind_name in (
                    ("BASE TABLE", False, "tables"),
                    ("VIEW", True, "views"),
                ):
                    self.user_discovery_logger.info(f"Fetching {object_kind_name} for schema '{schema_name}'...")
                    try:
                        schema_columns = self._fetch_schema_columns_bulk(schema_name, table_type, conn)
                    except Exception as e:
                        self.user_discovery_logger.error(
                            f"Skipping {object_kind_name} for schema '{schema_name}' due to error: {e}"
                        )
                        continue

                    # Apply table filtering
                    if filter_tables_config:
                        filter_names = self._get_filter_names(schema_name)
                        if filter_names:
                            filter_set = set(filter_names)
                            schema_columns = {t: c for t, c in schema_columns.items() if t in filter_set}
                        else:
                            original_count = len(schema_columns)
                            schema_columns = {
                                t: c for t, c in schema_columns.items()
                                if self._should_include_table(schema_name, t)
                            }
                            if original_count > len(schema_columns):
                                self.user_discovery_logger.info(
                                    f"Filtered {object_kind_name} for schema '{schema_name}' "
                                    f"from {original_count} to {len(schema_columns)} based on filter_tables config."
                                )

                    if not schema_columns:
                        self.user_discovery_logger.info(f"No {object_kind_name} discovered for schema '{schema_name}'.")
                        continue

                    self.user_discovery_logger.info(
                        f"Discovered {len(schema_columns)} {object_kind_name} for schema '{schema_name}'."
                    )

                    for table_name, columns in schema_columns.items():
                        try:
                            new_catalog_entry = self._build_catalog_entry(
                                schema_name=schema_name,
                                table_name=table_name,
                                is_view=is_view,
                                prefetched_columns=columns,
                                prefetched_pk=schema_pks.get(table_name, []),
                            ).to_dict()
                            result.append(new_catalog_entry)
                        except Exception as e:
                            self.user_discovery_logger.error(
                                f"Skipping table '{table_name}' of schema '{schema_name}' due to error: {e}"
                            )
                            continue

        if result:
            stream_list = "\n\t- " + "\n\t- ".join([catalog_entry.get("tap_stream_id") for catalog_entry in result])
            self.user_discovery_logger.info(f"Discovered streams ({len(result)}): {stream_list}")
        else:
            self.user_discovery_logger.error("No streams discovered, please check your configurations.")

        return result

    def _build_catalog_entry(
        self,
        schema_name: str,
        table_name: str,
        is_view: bool,  # noqa: FBT001
        prefetched_columns: list[tuple],
        prefetched_pk: list[str],
    ) -> CatalogEntry:
        """Build a CatalogEntry from prefetched bulk query data.

        Args:
            schema_name: Schema name.
            table_name: Table name.
            is_view: Whether this is a view.
            prefetched_columns: Column tuples from _fetch_schema_columns_bulk().
            prefetched_pk: PK column names from _fetch_schema_pks_bulk().
        """
        unique_stream_id = self.get_fully_qualified_name(
            db_name=None,
            schema_name=schema_name,
            table_name=table_name,
            delimiter="-",
        )

        key_properties = list(prefetched_pk) if prefetched_pk else []

        table_schema = th.PropertiesList()
        for col in prefetched_columns:
            col_name = col[0]       # COLUMN_NAME
            data_type = col[1]      # DATA_TYPE
            is_nullable = col[2]    # IS_NULLABLE ('YES'/'NO')
            char_max_len = col[3]   # CHARACTER_MAXIMUM_LENGTH
            num_precision = col[4]  # NUMERIC_PRECISION
            num_scale = col[5]      # NUMERIC_SCALE

            # Build type string for proper mapping
            if data_type in ("decimal", "numeric") and num_precision is not None:
                if num_scale is not None and num_scale > 0:
                    type_str = f"{data_type}({num_precision},{num_scale})"
                else:
                    type_str = f"{data_type}({num_precision})"
            elif data_type in ("varchar", "nvarchar", "char", "nchar") and char_max_len is not None:
                type_str = f"{data_type}({char_max_len})"
            else:
                type_str = data_type

            jsonschema_type = self.to_jsonschema_type(type_str)
            table_schema.append(
                th.Property(
                    name=col_name,
                    wrapped=th.CustomType(jsonschema_type),
                    required=col_name in key_properties,
                ),
            )
        schema = table_schema.to_dict()

        replication_method = "FULL_TABLE"

        return CatalogEntry(
            tap_stream_id=str(unique_stream_id),
            stream=str(unique_stream_id),
            table=table_name,
            key_properties=key_properties,
            schema=Schema.from_dict(schema),
            is_view=is_view,
            replication_method=replication_method,
            metadata=MetadataMapping.get_standard_metadata(
                schema_name=schema_name,
                schema=schema,
                replication_method=replication_method,
                key_properties=key_properties,
                valid_replication_keys=None,
            ),
            database=None,
            row_count=None,
            stream_alias=None,
            replication_key=None,
        )

    def discover_catalog_entry(
        self,
        engine: Engine,
        inspected: Inspector,
        schema_name: str | None,
        table_name: str,
        is_view: bool,  # noqa: FBT001
        *,
        reflected_columns: list[reflection.ReflectedColumn] | None = None,
        reflected_pk: reflection.ReflectedPrimaryKeyConstraint | None = None,
        reflected_indices: list[reflection.ReflectedIndex] | None = None,
    ) -> CatalogEntry:
        """Create `CatalogEntry` object for the given table or a view.

        Kept for compatibility with the parent class interface.
        """
        return super().discover_catalog_entry(
            engine,
            inspected,
            schema_name,
            table_name,
            is_view,
            reflected_columns=reflected_columns,
            reflected_pk=reflected_pk,
            reflected_indices=reflected_indices,
        )

    def discover_query_catalog_entry(
        self,
        stream_name: str,
        query: str,
    ) -> dict:
        """Discover catalog entry for a custom SQL query.

        Executes the query with no rows to introspect column types.

        Args:
            stream_name: The name for this query stream.
            query: The SQL query to introspect.

        Returns:
            A catalog entry dict for the query stream.
        """
        self.user_discovery_logger.info(f"Discovering query stream '{stream_name}'...")

        with self._engine.connect() as conn:
            # Execute with TOP 0 to get column metadata without fetching rows
            wrapped_query = sa.text(f"SELECT TOP 0 * FROM ({query}) AS _query_discovery")  # noqa: S608
            result = conn.execute(wrapped_query)
            cursor_description = result.cursor.description

        properties: dict[str, dict] = {}
        metadata_entries: list[dict] = []

        for col_desc in cursor_description:
            col_name = col_desc[0]
            col_type_code = col_desc[1]

            # Map pymssql type codes to JSON schema types
            # pymssql type codes: 1=STRING, 2=BINARY, 3=NUMBER, 4=DATETIME, 5=DECIMAL
            type_map = {
                1: {"type": ["string", "null"]},
                2: {"type": ["string", "null"]},
                3: {"type": ["number", "null"]},
                4: {"type": ["string", "null"], "format": "date-time"},
                5: {"type": ["number", "null"]},
            }
            col_schema = type_map.get(col_type_code, {"type": ["string", "null"]})

            properties[col_name] = col_schema
            metadata_entries.append({
                "breadcrumb": ["properties", col_name],
                "metadata": {"inclusion": "available"},
            })

        metadata_entries.append({
            "breadcrumb": [],
            "metadata": {
                "inclusion": "available",
                "table-key-properties": [],
                "forced-replication-method": "",
                "schema-name": "",
            },
        })

        catalog_entry = {
            "tap_stream_id": stream_name,
            "table_name": stream_name,
            "replication_method": "",
            "key_properties": [],
            "schema": {
                "properties": properties,
                "type": "object",
                "$schema": "https://json-schema.org/draft/2020-12/schema",
            },
            "is_view": False,
            "stream": stream_name,
            "metadata": metadata_entries,
        }

        self.user_discovery_logger.info(f"Discovered query stream '{stream_name}' with {len(properties)} columns.")
        return catalog_entry

    def get_sqlalchemy_type(self, col_meta_type: str) -> sa.Column:
        """Return a SQLAlchemy type object for the given SQL type.

        Used ischema_names so we don't have to manually map all types.
        """
        dialect = sa.dialects.mssql.base.dialect()  # type: ignore[attr-defined]
        ischema_names = dialect.ischema_names

        # Example varchar(97)
        type_info = col_meta_type.split("(")
        base_type_name = type_info[0].split(" ")[0]  # bigint unsigned
        type_args = type_info[1].split(" ")[0].rstrip(")") if len(type_info) > 1 else None

        type_class = ischema_names.get(base_type_name.lower())

        try:
            # Create an instance of the type class with parameters if they exist
            if type_args:
                return type_class(*map(int, type_args.split(",")))  # Want to create a varchar(97) if asked for
            return type_class()
        except Exception:
            self.logger.exception("Error creating sqlalchemy type for col_meta_type=%s", col_meta_type)
            raise

    def get_table_columns(
        self,
        full_table_name: str,
        column_names: list[str] | None = None,
    ) -> dict[str, sa.Column]:
        """Return a list of table columns.

        Args:
            full_table_name: Fully qualified table name.
            column_names: A list of column names to filter to.

        Returns:
            An ordered list of column objects.
        """
        return super().get_table_columns(full_table_name, column_names)

    def create_engine(self) -> Engine:
        try:
            # Enable TDS logging if requested (via environment variable)
            if self.config.get("enable_tds_logging"):
                from os import environ
                environ["TDSDUMP"] = "stderr"

            # Note: For pymssql, connection parameters (charset, tds_version, encryption, etc.)
            # are passed via URL query parameters in get_sqlalchemy_query(), not connect_args.
            # The pymssql dialect uses url.query to build the connection parameters.
            return sa.create_engine(
                self.sqlalchemy_url,
                echo=False,
                json_serializer=self.serialize_json,
                json_deserializer=self.deserialize_json,
                poolclass=QueuePool,
                pool_size=self.pool_size,
                max_overflow=self.pool_size * 2,
                pool_recycle=300,
                pool_pre_ping=True,
            )
        except TypeError:
            internal_logger.exception(
                "Retrying engine creation with fewer arguments due to TypeError.",
            )
            return sa.create_engine(
                self.sqlalchemy_url,
                echo=False,
            )
