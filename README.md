# AWS Lambda ETL

This repository implements a small, opinionated framework for building and running ETL pipes — modular extraction, transformation and load units that can run locally, in CI, or as AWS Lambda functions.

The core abstraction is the Pipe base class. Concrete pipes implement only a few methods, while the base class handles lifecycle, logging, connection management and standard loading behaviors.

## Architecture overview

```mermaid
flowchart TB
    schedule{{"Schedule trigger"}}
    api{{"API gateway"}}
    dwh[("DWH")]
    erp[("ERP")]

    subgraph "Pipe lambda function"
        pipe["Pipe.\_\_call\_\_()"]
        extract["Pipe.__extract()"]
        transform["Pipe.__transform()"]
        load["Pipe.__load()"]
    end

    subgraph "S3 bucket"
        file@{shape: doc, label: "CSV file"}
    end

    schedule -. event .-> pipe
    api -. event .-> pipe
    pipe -- parameters --> extract
    erp -- query --> extract
    dwh -- query --> extract
    extract -- raw data --> transform
    transform -- transformed data --> load
    load -- insert --> dwh
    load -- insert --> file
```

## Prerequisites

- Python 3.12+
- [AWS SAM CLI](https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/install-sam-cli.html)

## Local setup

Create a virtual environment for the project

```bash
# Create virtual env
python -m venv .venv
# Switch to venv
source .venv/bin/activate
# Install dependencies
pip install -r src/requirements.txt
# Load required static tables
python ./src/load_static.py
```

It is also strongly recommended to install the recommended VS Code extensions

## Run API locally

```bash
# 1. Build
sam build
# 2. Invoke locally
sam local invoke MyPipe
# 3. Run as an API endpoint
sam local start-api --env-vars scripts/sam_local_env.json
```

Bear in mind that you need to rebuild anytime there's a code change (`start-api` supports hot reloading so doesn't need to be restarted every time)

```
TODO add curl example and postman collection
```

## Development guidelines

### Database configurations

Database connections are configured in the `CONNECTIONS` global variable from `src.common.config`.
Each connection is a instance of the `common.database.Connection` dataclass.
Default values should be provided and should be the values for local development.

### Pipe definition

Each pipe should be a class inheriting from the `src.common.pipe.Pipe` base class.
That base class provides a lot of the boilerplate for extracting and loading the data as well as basic logging.
When writing a pipe, you'll mostly be overriding the static and/or abstract methods of the base class as detailed below.
See docstring for more details on how each method work.

#### Required

The following methods must be defined in each pipe

- `extract`: define how to extract data using SQL queries

#### Optional

- `output_destination`: define the destination of the output (database or S3 bucket)
- `schema`: define a schema for transformed data (Table or FlatFile)
- `connection`: define connection for loading data, if output destination is a database table
- `loading_method`: define how to load the data, if output destination is a database table
- `transform`: define how to transform the data

### Querying a database

It is important to use `with` clauses when querying the database to avoid leaving open connections (for instance in `extract` or `load` methods)

```python
with MySQL(CONNECTIONS["connection_name"]) as db:
    db.execute(text("""-- sql
            SELECT * FROM table_name
        """))
```

### Maintenance

To update dependencies, you can use [`pip-review`](https://github.com/jgonggrijp/pip-review): `pip-review --local --interactive`
