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

    def get_schema_names(self, engine: Engine, inspected: Inspector) -> list[str]:
        if "filter_dbs" in self.config and self.config["filter_dbs"]:
            return [s.strip() for s in self.config["filter_dbs"].split(",")]

        schemas = super().get_schema_names(engine, inspected)
        exclude_schemas = ["information_schema", "INFORMATION_SCHEMA", "performance_schema", "sys"]
        return [schema for schema in schemas if schema not in exclude_schemas]

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

        Args:
            engine: SQLAlchemy engine
            inspected: SQLAlchemy inspector instance for engine
            schema_name: Schema name to inspect
            table_name: Name of the table or a view
            is_view: Flag whether this object is a view, returned by `get_object_names`

        Returns:
            `CatalogEntry` object for the given table or a view
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
            connect_args = {}

            # Add charset if specified
            if self.config.get("characterset"):
                connect_args["charset"] = self.config["characterset"]

            # Add tds_version if specified
            if self.config.get("tds_version"):
                connect_args["tds_version"] = self.config["tds_version"]

            # Add conn_properties if specified
            if self.config.get("conn_properties"):
                connect_args["conn_properties"] = self.config["conn_properties"]

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
                connect_args=connect_args,
            )
        except TypeError:
            internal_logger.exception(
                "Retrying engine creation with fewer arguments due to TypeError.",
            )
            return sa.create_engine(
                self.sqlalchemy_url,
                echo=False,
            )
