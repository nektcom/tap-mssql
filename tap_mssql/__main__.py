"""CLI entry point for tap-mssql."""

from tap_mssql.tap import TapMSSQL

if __name__ == "__main__":
    TapMSSQL.cli()
