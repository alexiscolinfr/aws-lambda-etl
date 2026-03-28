import argparse
import os
import re
import sys
import tomllib
from contextlib import closing
from pathlib import Path

import pymysql
from dotenv import load_dotenv
from sshtunnel import SSHTunnelForwarder


_CONFIG_PATH = Path(__file__).parent / "migration_tables.toml"


load_dotenv()

BATCH_SIZE = 100_000
SSH_TUNNEL_PORT = 3308

SYSTEM_ENV_PREFIXES: dict[str, str] = {
    "dwh": "DWH",
    "erp": "ERP",
}

_VALID_TABLE_NAME = re.compile(r"^[a-zA-Z0-9_]+$")


class UnknownSystemError(Exception):
    def __init__(self, system: str, available: list[str]):
        super().__init__(f"Unknown system: {system!r}. Available: {available}")


def get_connection_params(system: str) -> dict[str, str | int]:
    """Returns connection parameters based on the selected system."""
    prefix = SYSTEM_ENV_PREFIXES.get(system)
    if prefix is None:
        raise UnknownSystemError(system, list(SYSTEM_ENV_PREFIXES))

    return {
        "bastion_host": os.getenv("BASTION_HOST"),
        "bastion_port": int(os.getenv("BASTION_PORT", "22")),
        "bastion_user": os.getenv("BASTION_USER"),
        "bastion_key_path": os.getenv("BASTION_KEY_PATH"),
        "prod_db_host": os.getenv(f"{prefix}_PROD_DB_HOST"),
        "prod_db_port": int(os.getenv(f"{prefix}_PROD_DB_PORT", "3306")),
        "prod_db_user": os.getenv(f"{prefix}_PROD_DB_USER"),
        "prod_db_password": os.getenv(f"{prefix}_PROD_DB_PASSWORD"),
        "prod_db_name": os.getenv(f"{prefix}_PROD_DB_NAME"),
        "local_db_host": os.getenv(f"{prefix}_LOCAL_DB_HOST", "localhost"),
        "local_db_port": int(os.getenv(f"{prefix}_LOCAL_DB_PORT", "3306")),
        "local_db_user": os.getenv(f"{prefix}_LOCAL_DB_USER"),
        "local_db_password": os.getenv(f"{prefix}_LOCAL_DB_PASSWORD"),
        "local_db_name": os.getenv(f"{prefix}_LOCAL_DB_NAME"),
    }


class InvalidTableNameError(Exception):
    def __init__(self, table_name: str):
        super().__init__(f"Invalid table name: {table_name!r}")


def _validate_table_name(table_name: str) -> None:
    """Ensures the table name is safe to interpolate in SQL queries."""
    if not _VALID_TABLE_NAME.match(table_name):
        raise InvalidTableNameError(table_name)


def escape_column_name(column_name: str) -> str:
    """Escapes a column name w/ backticks to avoid conflicts with reserved keywords."""
    return f"`{column_name}`"


def create_or_truncate_table(
    cursor_prod: pymysql.cursors.Cursor,
    cursor_local: pymysql.cursors.Cursor,
    table_name: str,
) -> None:
    """Creates the table in the local database if it doesn't exist, or truncates it."""
    cursor_local.execute(f"SHOW TABLES LIKE '{table_name}'")
    result = cursor_local.fetchone()

    if not result:
        cursor_prod.execute(f"SHOW CREATE TABLE {table_name}")
        create_table_query = cursor_prod.fetchone()[1]
        cursor_local.execute(create_table_query)
        print(f"    Table {table_name} created in the local database.")
    else:
        cursor_local.execute(f"TRUNCATE TABLE {table_name}")
        print(f"    Table {table_name} already exists and has been truncated.")


def get_regular_columns(cursor: pymysql.cursors.Cursor, table_name: str) -> list[str]:
    """Returns column names in table order, excluding generated (virtual/stored) cols.

    Generated columns cannot be targeted by INSERT statements — MySQL recomputes
    them automatically from their expression, so we simply omit them.
    """
    cursor.execute(f"SHOW COLUMNS FROM {table_name}")
    return [
        row[0] for row in cursor.fetchall() if "GENERATED" not in (row[5] or "").upper()
    ]


def migrate_table_in_batches(
    cursor_prod: pymysql.cursors.Cursor,
    cursor_local: pymysql.cursors.Cursor,
    table_name: str,
) -> None:
    """Migrates data from the production database to the local database in batches."""
    _validate_table_name(table_name)

    print(f"Starting migration for table {table_name}:")

    cursor_local.execute("SET FOREIGN_KEY_CHECKS = 0")
    cursor_local.execute("SET SESSION sql_mode = 'NO_ENGINE_SUBSTITUTION'")

    try:
        create_or_truncate_table(cursor_prod, cursor_local, table_name)

        cursor_prod.execute(f"SELECT COUNT(*) FROM {table_name}")  # noqa: S608
        total_rows = cursor_prod.fetchone()[0]

        if total_rows == 0:
            print(f"    No data to migrate for table {table_name}.")
            return

        # Exclude generated columns: MySQL forbids inserting values into them.
        # The local DB recomputes them automatically from their stored expression.
        regular_cols = get_regular_columns(cursor_prod, table_name)
        escaped_cols = [escape_column_name(col) for col in regular_cols]
        columns_str = ", ".join(escaped_cols)
        values_placeholder = ", ".join(["%s"] * len(escaped_cols))
        insert_query = (
            f"INSERT INTO {table_name} ({columns_str}) VALUES ({values_placeholder})"  # noqa: S608
        )

        offset = 0
        while offset < total_rows:
            cursor_prod.execute(
                f"SELECT {columns_str} FROM {table_name} LIMIT {BATCH_SIZE} OFFSET {offset}"  # noqa: S608
            )
            rows = cursor_prod.fetchall()

            if not rows:
                break

            cursor_local.executemany(insert_query, rows)
            offset += len(rows)
            percent = int((offset / total_rows) * 100)
            print(
                f"    {percent:3d}% rows migrated ({offset:,}/{total_rows:,})",
                end="\r",
                flush=True,
            )
    finally:
        cursor_local.execute("SET FOREIGN_KEY_CHECKS = 1")

    print(f"Completed migration for table {table_name}, total rows: {offset:,}   ")


def establish_ssh_tunnel(params: dict[str, str | int]) -> SSHTunnelForwarder:
    """Establishes an SSH tunnel to the production database."""
    return SSHTunnelForwarder(
        (params["bastion_host"], params["bastion_port"]),
        ssh_username=params["bastion_user"],
        ssh_private_key=params["bastion_key_path"],
        remote_bind_address=(params["prod_db_host"], params["prod_db_port"]),
        local_bind_address=("127.0.0.1", SSH_TUNNEL_PORT),
    )


def migrate_tables(system: str, tables: list[str]) -> None:
    """Migrates the given tables from the production database to the local database."""
    params = get_connection_params(system)

    with (
        closing(
            pymysql.connect(
                host=params["local_db_host"],
                port=params["local_db_port"],
                user=params["local_db_user"],
                password=params["local_db_password"],
                db=params["local_db_name"],
            )
        ) as connection_local,
        establish_ssh_tunnel(params) as _tunnel,
    ):
        print(
            f"SSH tunnel established to {params['prod_db_host']}"
            f" on local port {SSH_TUNNEL_PORT}"
        )

        with (
            closing(
                pymysql.connect(
                    host="127.0.0.1",
                    port=SSH_TUNNEL_PORT,
                    user=params["prod_db_user"],
                    password=params["prod_db_password"],
                    db=params["prod_db_name"],
                )
            ) as connection_remote,
            connection_remote.cursor() as cursor_prod,
            connection_local.cursor() as cursor_local,
        ):
            for table in tables:
                migrate_table_in_batches(cursor_prod, cursor_local, table)
                connection_local.commit()

            print(f"Migration completed for tables: {tables}")


def load_table_config() -> dict[str, dict[str, list[str]]]:
    """Loads the table configuration from the TOML file."""
    with _CONFIG_PATH.open("rb") as f:
        return tomllib.load(f)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Migrate tables from a production database to the local database.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
    examples:
    # Migrate all configured tables for a system
    python update_local_db.py --system dwh

    # Migrate a named group of tables (defined in migration_tables.toml)
    python update_local_db.py --system dwh --group dim_tables

    # Migrate specific tables only
    python update_local_db.py --system erp --tables orders products
        """,
    )
    parser.add_argument(
        "--system",
        required=True,
        choices=list(SYSTEM_ENV_PREFIXES),
        metavar="SYSTEM",
        help=f"System to migrate from. Choices: {list(SYSTEM_ENV_PREFIXES)}",
    )
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "--tables",
        nargs="+",
        metavar="TABLE",
        help="Specific tables to migrate (overrides config file).",
    )
    group.add_argument(
        "--group",
        metavar="GROUP",
        help="Named group of tables defined in migration_tables.toml (e.g. 'dim_tables').",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()

    if args.tables:
        tables = args.tables
    else:
        config = load_table_config()
        system_config = config.get(args.system, {})
        group = args.group or "tables"

        if group not in system_config:
            available = list(system_config.keys())
            print(
                f"Error: group '{group}' not found for system '{args.system}'. "
                f"Available groups: {available}",
                file=sys.stderr,
            )
            sys.exit(1)

        tables = system_config[group]

    migrate_tables(args.system, tables)
