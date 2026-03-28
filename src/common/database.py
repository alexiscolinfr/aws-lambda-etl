from dataclasses import dataclass
from enum import Enum
from typing import assert_never
from urllib.parse import quote_plus

from sqlalchemy import MetaData, create_engine


class DatabaseSystem(Enum):
    POSTGRES = "postgres"
    MYSQL = "mysql"


@dataclass
class Connection:
    host: str
    port: int
    user: str
    password: str
    metadata: MetaData
    system: DatabaseSystem
    database: str | None = None

    def to_url(self) -> str:
        encoded_password = quote_plus(self.password)
        match self.system:
            case DatabaseSystem.POSTGRES:
                return f"postgresql+psycopg://{self.user}:{encoded_password}@{self.host}:{self.port}/{self.database or ''}"
            case DatabaseSystem.MYSQL:
                return f"mysql+mysqlconnector://{self.user}:{encoded_password}@{self.host}:{self.port}/{self.database or ''}"
            case _ as unreachable:
                assert_never(unreachable)


class Database:
    def __init__(self, connection: Connection):
        self.db = create_engine(
            connection.to_url(), connect_args={"connect_timeout": 300}
        ).connect()

    def __enter__(self):
        return self.db

    def __exit__(self, exc_type, value, traceback):
        if traceback is None:
            self.db.commit()
        else:
            self.db.rollback()
        self.db.close()
