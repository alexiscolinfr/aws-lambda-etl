import os
import sys
from pathlib import Path

import dotenv
from sqlalchemy import SQLAlchemyError, text


# Add src to sys.path
SRC_DIR = Path(__file__).parent.parent / "src"
sys.path.insert(0, str(SRC_DIR))

from common.database import Connection, Database, DatabaseSystem  # noqa: E402


"""
Apply all DDL statements from .sql files in src/static/ddl
to the reporting database.
"""

dotenv_path = Path(__file__).parent.parent / ".env"
if dotenv_path.exists():
    dotenv.load_dotenv(dotenv_path)
else:
    print(".env file not found, relying on environment variables.")

DDL_DIR = SRC_DIR / "static" / "ddl"


def load_tables():
    print("Applying DDL to database...")

    connection = Connection(
        system=DatabaseSystem.MYSQL,
        host=os.environ.get("DWH_HOST"),
        port=os.environ.get("DWH_PORT"),
        user=os.environ.get("DWH_USER"),
        password=os.environ.get("DWH_PASSWORD"),
        database=os.environ.get("DWH_DATABASE"),
        metadata=None,
    )

    sql_files = sorted(DDL_DIR.glob("*.sql"))
    if not sql_files:
        print(f"No .sql files found in {DDL_DIR}")
        return

    with Database(connection) as db:
        for sql_file in sql_files:
            print(f"Executing {sql_file.name}...")
            with Path.open(sql_file, "r", encoding="utf-8") as f:
                queries = f.read().split(";")
                for q in queries:
                    stripped_q = q.strip()
                    if stripped_q:
                        try:
                            db.execute(text(stripped_q))
                        except SQLAlchemyError as e:
                            print(f"Failed executing query from {sql_file.name}:\n{e}")

    print("DDL applied successfully.")


if __name__ == "__main__":
    load_tables()
