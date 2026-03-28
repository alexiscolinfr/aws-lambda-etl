import os

from dotenv import load_dotenv
from sqlalchemy import MetaData

from common.database import Connection, DatabaseSystem


load_dotenv()

CONNECTIONS = {
    "data_warehouse": Connection(
        system=DatabaseSystem.MYSQL,
        host=os.getenv("DWH_HOST", "localhost"),
        port=int(os.getenv("DWH_PORT", "3306")),
        user=os.getenv("DWH_USER", "db_user"),
        password=os.getenv("DWH_PASSWORD", "db_password"),
        database=os.getenv("DWH_DATABASE", "db_name"),
        metadata=MetaData(),
    ),
    "erp": Connection(
        system=DatabaseSystem.MYSQL,
        host=os.getenv("ERP_HOST", "localhost"),
        port=int(os.getenv("ERP_PORT", "3306")),
        user=os.getenv("ERP_USER", "db_user"),
        password=os.getenv("ERP_PASSWORD", "db_password"),
        database=os.getenv("ERP_DATABASE", "db_name"),
        metadata=MetaData(),
    ),
}
