# AWS Lambda ETL

> Collection of data ETL pipelines deployed as AWS Lambda functions.

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

---

## Getting started

### Prerequisites

- [Python 3.12+](https://www.python.org/downloads/)
- [uv](https://docs.astral.sh/uv/getting-started/installation/)
- [AWS SAM CLI](https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/install-sam-cli.html)

### Setup

```bash
# Install dependencies and create virtual environment
uv sync

# Install pre-commit hooks
uv run pre-commit install

# Copy and fill in environment variables
cp .env.example .env
```

### Apply DDL

Create the `pipeline_logs` table and any other DDL defined in `src/static/ddl/`:

```bash
uv run python scripts/apply_static_ddl.py
```

---

## Running pipes locally

```bash
uv run python scripts/run_local.py
```

An interactive menu lets you select which pipe(s) to run, individually or by group.
Debug mode redirects output to `.output/<pipe_path>.csv` instead of loading to the database.

## Running the API locally

```bash
sam build
sam local invoke MyPipe
# or as an API endpoint
sam local start-api --env-vars scripts/sam_local_env.json
```

> [!NOTE]
> You need to rebuild after each code change. `start-api` supports hot reloading and does not need to be restarted.

---

## Development tools

### VS Code extensions

| Extension | Purpose |
|:---|:---|
| [Python](https://marketplace.visualstudio.com/items?itemName=ms-python.python) | Python language support |
| [Ruff](https://marketplace.visualstudio.com/items?itemName=charliermarsh.ruff) | Linting and formatting |
| [Inline SQL](https://marketplace.visualstudio.com/items?itemName=qufiwefefwoyn.inline-sql-syntax) | SQL syntax highlighting in Python strings |
| [Code Spell Checker](https://marketplace.visualstudio.com/items?itemName=streetsidesoftware.code-spell-checker) | Spell checking |

### Formatting & linting

This project uses [Ruff](https://docs.astral.sh/ruff/) for formatting and linting. It runs automatically on staged files at each commit via pre-commit.

```bash
uv run ruff format .        # Format
uv run ruff check . --fix   # Lint with auto-fix

uv run pre-commit run --all-files  # Run all hooks manually
```

### Dependency management

This project uses [uv](https://docs.astral.sh/uv/) for dependency management.

```bash
uv add <package>       # Add a runtime dependency
uv add --dev <package> # Add a dev dependency
uv sync --upgrade      # Update all dependencies
```

After updating dependencies, regenerate the Lambda layer requirements:

```bash
uv export --no-dev --no-hashes -o layer/requirements.txt
```

---

## Development guidelines

### Project structure

```
src/
├── common/                    # Shared utilities and base classes
│   ├── config.py              # Database connections
│   ├── database.py            # Database connection wrapper
│   ├── email_sender.py        # SES email sender
│   ├── exceptions.py          # Custom exceptions
│   ├── flat_file.py           # FlatFile schema (for S3/email outputs)
│   ├── json_loader.py         # JSON → DataFrame loader
│   ├── pipe.py                # Pipe base class
│   ├── transformation_tools.py
│   └── enums/                 # LoadingMethod, OutputDestination, Status
├── pipes/                     # Pipeline implementations
│   ├── chatbot/
│   ├── data_extraction/
│   ├── dimensions/
│   ├── facts/
│   └── pricing/
└── static/
    ├── data/                  # JSON model/config files
    ├── ddl/                   # SQL DDL scripts (applied via apply_static_ddl.py)
    └── templates/             # Email HTML templates
```

### Database configurations

Database connections are configured in the `CONNECTIONS` global variable from `common.config`. Each connection is an instance of the `common.database.Connection` dataclass. Default values should be the values for local development.

### Pipe definition

Each pipe is a class inheriting from `common.pipe.Pipe`. The base class handles ETL orchestration, logging, error handling, and the Lambda entrypoint — you only need to define the data-specific parts.

```python
@dataclass
class MyPipeParameters:
    # Parameters passed via the Lambda event body (JSON)
    my_param: str = "default_value"


class MyPipe(Pipe):
    parameter_class = MyPipeParameters
    output_destination = OutputDestination.DATABASE
    connection = CONNECTIONS["data_warehouse"]
    loading_method = LoadingMethod.DROP_INSERT
    schema = Table(
        "my_table",
        connection.metadata,
        Column("id", Integer, primary_key=True),
        Column("name", String(255)),
    )

    @staticmethod
    def extract(parameters: MyPipeParameters) -> dict[str, DataFrame]:
        with Database(CONNECTIONS["erp"]) as db:
            return {"data": db.execute(text("SELECT ..."))}

    @staticmethod
    def transform(data: dict[str, DataFrame], parameters: MyPipeParameters) -> DataFrame:
        # Optional — omit if no transformation needed
        return data["data"]


handle = MyPipe()
```

#### Class attributes

| Attribute | Required | Description |
|:---|:---:|:---|
| `parameter_class` | ✅ | Dataclass defining the pipe's input parameters |
| `output_destination` | ✅ | `DATABASE`, `S3`, `EMAIL`, or `S3_EMAIL` |
| `schema` | ✅ | SQLAlchemy `Table` or `FlatFile` |
| `connection` | If DATABASE | Target database connection |
| `loading_method` | If DATABASE | `DROP_INSERT`, `TRUNCATE_INSERT`, or `INSERT` |

#### Methods

| Method | Required | Description |
|:---|:---:|:---|
| `extract` | ✅ | Fetch raw data — returns `dict[str, DataFrame]` |
| `transform` | ➖ | Transform extracted data — returns `DataFrame` |

### Adding a new pipe

1. Create a file in the appropriate `src/pipes/<category>/` subdirectory
2. Define the parameters dataclass and pipe class (see template above)
3. Add the corresponding Lambda function to `template.yaml`

### Updating the local database

Mirror production tables locally via SSH tunnel:

```bash
# Sync all configured tables for a system
uv run python scripts/update_local_db.py --system dwh

# Sync a named group only
uv run python scripts/update_local_db.py --system dwh --group dim_tables

# Sync specific tables
uv run python scripts/update_local_db.py --system erp --tables orders products
```

Table groups are defined in `scripts/migration_tables.toml`.
