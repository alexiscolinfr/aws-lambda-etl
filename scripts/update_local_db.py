import os

import pymysql
from dotenv import load_dotenv
from sshtunnel import SSHTunnelForwarder

# Load environment variables from .env file
load_dotenv()

# Constants
BATCH_SIZE = 100000
SSH_TUNNEL_PORT = 3308


def get_connection_params(system):
    """Returns connection parameters based on the selected system."""
    common_params = {
        "bastion_host": os.getenv("BASTION_HOST"),
        "bastion_port": int(os.getenv("BASTION_PORT", 22)),
        "bastion_user": os.getenv("BASTION_USER"),
        "bastion_key_path": os.getenv("BASTION_KEY_PATH"),
    }

    if system == "Data Warehouse":
        return {
            **common_params,
            **{
                "prod_db_host": os.getenv("DWH_PROD_DB_HOST"),
                "prod_db_port": int(os.getenv("DWH_PROD_DB_PORT", 3306)),
                "prod_db_user": os.getenv("DWH_PROD_DB_USER"),
                "prod_db_password": os.getenv("DWH_PROD_DB_PASSWORD"),
                "prod_db_name": os.getenv("DWH_PROD_DB_NAME"),
                "local_db_host": os.getenv("DWH_LOCAL_DB_HOST", "localhost"),
                "local_db_port": int(os.getenv("DWH_LOCAL_DB_PORT", 3306)),
                "local_db_user": os.getenv("DWH_LOCAL_DB_USER"),
                "local_db_password": os.getenv("DWH_LOCAL_DB_PASSWORD"),
                "local_db_name": os.getenv("DWH_LOCAL_DB_NAME"),
            },
        }
    elif system == "ERP":
        return {
            **common_params,
            **{
                "prod_db_host": os.getenv("ERP_PROD_DB_HOST"),
                "prod_db_port": int(os.getenv("ERP_PROD_DB_PORT", 3306)),
                "prod_db_user": os.getenv("ERP_PROD_DB_USER"),
                "prod_db_password": os.getenv("ERP_PROD_DB_PASSWORD"),
                "prod_db_name": os.getenv("ERP_PROD_DB_NAME"),
                "local_db_host": os.getenv("ERP_LOCAL_DB_HOST", "localhost"),
                "local_db_port": int(os.getenv("ERP_LOCAL_DB_PORT", 3306)),
                "local_db_user": os.getenv("ERP_LOCAL_DB_USER"),
                "local_db_password": os.getenv("ERP_LOCAL_DB_PASSWORD"),
                "local_db_name": os.getenv("ERP_LOCAL_DB_NAME"),
            },
        }
    else:
        raise ValueError(f"Unknown system: {system}")


def escape_column_name(column_name):
    """Escapes column names using backticks to avoid issues with reserved keywords."""
    return f"`{column_name}`"


def create_or_truncate_table(cursor_prod, cursor_local, table_name):
    """Creates a table in the local database if it doesn't exist, using the structure from the production database."""
    cursor_local.execute(f"SHOW TABLES LIKE '{table_name}'")
    result = cursor_local.fetchone()

    if not result:
        cursor_prod.execute(f"SHOW CREATE TABLE {table_name}")
        create_table_query = cursor_prod.fetchone()[1]
        cursor_local.execute(create_table_query)
        print(f"Table {table_name} created in the local database.")
    else:
        cursor_local.execute(f"TRUNCATE TABLE {table_name}")
        print(
            f"Table {table_name} already exists in the local database and has been truncated."
        )


def migrate_table_in_batches(cursor_prod, cursor_local, table_name):
    """Migrates data from production to local database in batches."""
    cursor_local.execute("SET FOREIGN_KEY_CHECKS = 0")
    cursor_local.execute("SET SESSION sql_mode = 'NO_ENGINE_SUBSTITUTION'")
    create_or_truncate_table(cursor_prod, cursor_local, table_name)

    cursor_prod.execute(f"SELECT COUNT(*) FROM {table_name}")
    total_rows = cursor_prod.fetchone()[0]
    print(f"Total rows to migrate for table {table_name}: {total_rows}")

    if total_rows == 0:
        print(f"No data to migrate for table {table_name}")
        return

    cursor_prod.execute(f"SELECT * FROM {table_name} LIMIT 1")
    column_names = [escape_column_name(desc[0]) for desc in cursor_prod.description]
    columns_str = ", ".join(column_names)
    values_placeholder = ", ".join(["%s"] * len(column_names))

    offset = 0
    while offset < total_rows:
        cursor_prod.execute(
            f"SELECT * FROM {table_name} LIMIT {BATCH_SIZE} OFFSET {offset}"
        )
        rows = cursor_prod.fetchall()

        if not rows:
            break

        insert_query = (
            f"INSERT INTO {table_name} ({columns_str}) VALUES ({values_placeholder})"
        )
        cursor_local.executemany(insert_query, rows)
        print(f"Migrated {len(rows)} rows from {table_name}, offset {offset}")
        offset += len(rows)

    cursor_local.execute("SET FOREIGN_KEY_CHECKS = 1")
    print(f"Completed migration for table {table_name}, total rows: {offset}")


def establish_ssh_tunnel(params):
    """Establishes an SSH tunnel to the production database."""
    return SSHTunnelForwarder(
        (params["bastion_host"], params["bastion_port"]),
        ssh_username=params["bastion_user"],
        ssh_private_key=params["bastion_key_path"],
        remote_bind_address=(params["prod_db_host"], params["prod_db_port"]),
        local_bind_address=("127.0.0.1", SSH_TUNNEL_PORT),
    )


def migrate_tables(system, tables):
    """Main function to handle migration of tables."""
    params = get_connection_params(system)

    # Connect to local database
    connection_local = pymysql.connect(
        host=params["local_db_host"],
        port=params["local_db_port"],
        user=params["local_db_user"],
        password=params["local_db_password"],
        db=params["local_db_name"],
    )

    try:
        with establish_ssh_tunnel(params) as tunnel:
            print(
                f"SSH tunnel established to {params['prod_db_host']} on local port {SSH_TUNNEL_PORT}"
            )
            connection_remote = pymysql.connect(
                host="127.0.0.1",
                port=SSH_TUNNEL_PORT,
                user=params["prod_db_user"],
                password=params["prod_db_password"],
                db=params["prod_db_name"],
            )

            try:
                with connection_remote.cursor() as cursor_prod, connection_local.cursor() as cursor_local:
                    for table in tables:
                        print(f"Starting migration for table {table}")
                        migrate_table_in_batches(cursor_prod, cursor_local, table)
                        connection_local.commit()
                    print(f"Migration completed for tables: {tables}")
            finally:
                connection_remote.close()
                print("Remote connection closed.")
    except Exception as e:
        print(f"Error during migration: {e}")
    finally:
        connection_local.close()
        print("Local connection closed.")


if __name__ == "__main__":

    dwh_tables = [
        "dim_date",
        "fact_inventory",
    ]

    # migrate_tables("Data Warehouse", dwh_tables)
