# Load log schemas and (mostly) static tables into the reporting databases
import os

import dotenv
from sqlalchemy import text

from common.database import Connexion, Database

environments = ["local"]
files = ["log/schema.sql"]


def load_table(environment: str):
    dotenv.load_dotenv(f"src/static/.env.{environment}")
    connexion = Connexion(
        host=os.environ.get("DWH_HOST"),
        port=os.environ.get("DWH_PORT"),
        user=os.environ.get("DWH_USER"),
        password=os.environ.get("DWH_PASSWORD"),
        database=os.environ.get("DWH_DATABASE"),
    )

    for f in files:
        path = os.path.join(os.path.dirname(os.path.realpath(__file__)), "static", f)
        print(path)
        with open(path) as queriesFile:
            queries = queriesFile.read().split(";")
            for q in queries:
                with Database(connexion) as db:
                    print(q)
                    db.execute(text(q))


for env in environments:
    load_table(env)
