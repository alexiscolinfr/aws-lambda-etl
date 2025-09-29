from dataclasses import dataclass
from enum import Enum
from typing import Optional
from urllib.parse import quote_plus

from sqlalchemy import MetaData, create_engine


class DatabaseSystem(Enum):
    POSTGRES = "postgres"
    MYSQL = "mysql"


@dataclass
class Connexion:
    host: str
    port: int
    user: str
    password: str
    metadata: MetaData
    system: DatabaseSystem
    database: Optional[str] = None

    def to_url(self) -> str:
        encodedPassword = quote_plus(self.password)
        match self.system:
            case DatabaseSystem.POSTGRES:
                return f"postgresql+psycopg://{self.user}:{encodedPassword}@{self.host}:{self.port}/{self.database or ''}"
            case DatabaseSystem.MYSQL:
                return f"mysql+mysqlconnector://{self.user}:{encodedPassword}@{self.host}:{self.port}/{self.database or ''}"
            case _:
                raise ValueError("Invalid database system")


class Database(object):
    def __init__(self, connexion: Connexion):
        self.db = create_engine(
            connexion.to_url(), connect_args={"connect_timeout": 300}
        ).connect()

    def __enter__(self):
        return self.db

    def __exit__(self, type, value, traceback):
        if traceback is None:
            self.db.commit()
        else:
            self.db.rollback()
        self.db.close()
