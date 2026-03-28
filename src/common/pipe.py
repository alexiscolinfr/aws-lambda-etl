import json
import os
import time
import uuid
from abc import ABC, abstractmethod
from dataclasses import asdict
from io import StringIO
from pathlib import Path
from typing import Any

import boto3
from aws_lambda_typing import events
from pandas import NA, DataFrame
from sqlalchemy import Table, text

from common.config import CONNECTIONS
from common.database import Connection, Database
from common.email_sender import EmailSender
from common.enums.loading_method import LoadingMethod
from common.enums.output_destination import OutputDestination
from common.enums.status import Status
from common.exceptions import (
    BasePathNotInitializedError,
    ConnectionFailedError,
    NotNullViolationError,
    PrimaryKeyViolationError,
    TableNotFoundError,
)
from common.flat_file import FlatFile


class Pipe(ABC):
    """Base class for creating pipelines

    Children classes can be called as AWS Lambda function as follows:
        `MyPipe()(event, context)`

    To debug you can pass the `debug=True` argument on initialization.
        `MyPipe(debug=True)({}, {})`
    The transformed data will be loaded to a CSV file in the `.output` folder
    instead of the database.
    """

    parameter_class: Any
    output_destination: OutputDestination
    schema: FlatFile | Table
    base_path: Path | None = None

    # For database output
    connection: Connection | None = None
    loading_method: LoadingMethod | None = None

    def __init__(self, debug: bool = False) -> None:
        self._debug = debug
        self._parameters: dict[str, Any] = {}
        self._uuid: str = ""
        self._manual_trigger: bool = False
        self._start_time: float = 0.0
        self._extracted_rows: int = 0
        self._total_memory: float = 0.0
        self._loaded_rows: int = 0

    def __call__(
        self, event: events.EventBridgeEvent | events.APIGatewayProxyEventV2, _
    ) -> None:
        print(f"Event data: {event}")
        self._parameters = json.loads(event.get("body") or "{}")
        self._manual_trigger = not (
            event is not None and event.get("source") == "aws.scheduler"
        )
        print(f"Parameters: {asdict(self.parameters)}")
        if Pipe.base_path is None:
            Pipe.base_path = (
                Path() if os.environ.get("AWS_EXECUTION_ENV") else Path("src")
            )
        self._uuid = event.get("id", str(uuid.uuid4()))
        self._start_time = time.time()
        try:
            self.__log(Status.STARTED, None)
            self.__handle()
            self.__log(Status.COMPLETED, None)
        except Exception as e:
            self.__log(Status.FAILED, str(e))
            raise  # Re-raise the exception to log in AWS

    @property
    def _path(self) -> str:
        return f"{self.__class__.__module__}.{self.__class__.__name__}"

    @property
    def parameters(self) -> object:
        return self.parameter_class(**self._parameters)

    @classmethod
    def get_base_path(cls) -> Path:
        if cls.base_path is None:
            raise BasePathNotInitializedError
        return cls.base_path

    @staticmethod
    @abstractmethod
    def extract(parameters: object) -> dict[str, DataFrame]:
        """Define how to extract the data. Usually consists of one or more SQL queries

        Return a dictionary with the dataframes to be transformed
        If there's no transformations to do, return the dataframe in the `'data'` key
        """

    @staticmethod
    def transform(data: dict[str, DataFrame], _parameters: object) -> DataFrame:
        """
        Define how to transform the data.
        Usually consists of data manipulation on one or more dataframes.

        If there's no transformations to do, use the default implementation
        that returns the `'data'` key.
        """
        return data["data"]

    @staticmethod
    def load_to_db(
        data: DataFrame,
        connection: Connection,
        table: Table,
        loading_method: LoadingMethod,
    ) -> None:
        """Define how to load the transformed data

        Default implementation loads the transformed data to the table defined
        in the `table` method.
        """

        with Database(connection) as db:
            match loading_method:
                case LoadingMethod.DROP_INSERT:
                    table.drop(db, checkfirst=True)
                    table.create(db)
                case LoadingMethod.TRUNCATE_INSERT:
                    if db.dialect.has_table(db, table.name):
                        db.execute(text(f"TRUNCATE TABLE {table.name}"))
                    else:
                        raise TableNotFoundError(table.name)
                case LoadingMethod.INSERT:
                    table.create(db, checkfirst=True)

            data.to_sql(
                name=table.name,
                con=db,
                index=False,
                if_exists="append",
                chunksize=20000,
            )

    @staticmethod
    def load_to_s3(csv_bytes: bytes, bucket: str, key: str) -> None:
        """Uploads CSV bytes to the specified S3 bucket."""
        s3 = boto3.client("s3")
        s3.put_object(Bucket=bucket, Key=key, Body=csv_bytes)

    def __handle(self, *_args: Any, **_kwargs: Any) -> Any:
        print(f"Pipe {self._path} started at {time.strftime('%Y-%m-%d %H:%M:%S')}")

        self.__test_connection()

        if self._debug:
            self.__debug(self.__transform(self.__extract()))
        else:
            self.__load(self.__transform(self.__extract()))

        duration = time.time() - self._start_time
        minutes, seconds = divmod(duration, 60)

        print(
            f"Pipe {self._path} finished at {time.strftime('%Y-%m-%d %H:%M:%S')} in "
            f"{f'{int(minutes)} minutes and ' if minutes > 0 else ''}{seconds:.2f} seconds"
        )

    def __extract(self) -> dict[str, DataFrame]:
        print("--- Extracting ---")

        extracted_data = self.__class__.extract(self.parameters)

        for key, df in extracted_data.items():
            if df.shape[0] == 0:
                print(f"[Warning] Extracted {key} DataFrame is empty.")
            else:
                print(
                    f"Extracted {key} DataFrame: {df.shape[0]} rows x {df.shape[1]} columns, {df.memory_usage(deep=True).sum() / 1024**2:.2f} MB"
                )

        memory = sum(
            [df.memory_usage(deep=True).sum() for df in extracted_data.values()]
        )
        self._total_memory += memory
        self._extracted_rows = sum([df.shape[0] for df in extracted_data.values()])

        print(f"Total memory usage: {memory / 1024**2:.2f} MB")

        return extracted_data

    def __transform(self, extracted_data: dict[str, DataFrame]) -> DataFrame:
        print("--- Transforming ---")

        transformed_data = self.__class__.transform(extracted_data, self.parameters)
        schema_columns = self.__get_schema_columns()
        transformed_data = self.__clean_string(transformed_data[schema_columns]).fillna(
            self.__get_default_values()
        )

        memory = transformed_data.memory_usage(deep=True).sum()
        self._total_memory += memory

        print(
            f"Transformed output DataFrame: {transformed_data.shape[0]} rows x {transformed_data.shape[1]} columns, {memory / 1024**2:.2f} MB"
        )

        return transformed_data

    def __load(self, data: DataFrame) -> None:
        print("--- Loading ---")

        destination = self.output_destination

        if destination.uses_db:
            self.__class__.load_to_db(
                data,
                self.connection,
                self.schema,
                self.loading_method,
            )
            print(f"Output loaded to {self.schema.name} table")
        else:
            self.__validate_schema(data)
            csv_buffer = StringIO()
            data.to_csv(csv_buffer, index=False)
            csv_bytes = csv_buffer.getvalue().encode("utf-8")

            if destination.uses_s3:
                bucket = self.parameters.s3_bucket
                key = self.parameters.s3_key
                self.__class__.load_to_s3(csv_bytes, bucket, key)
                print(f"Output loaded to s3://{bucket}/{key}")

            if destination.uses_email:
                self.__send_email(data, csv_bytes)

        self._loaded_rows = data.shape[0]

    def __get_schema_columns(self) -> list[str]:
        if self.output_destination.uses_db:
            return [col.name for col in self.schema.columns]
        return self.schema.columns

    def __get_default_values(self) -> dict[str, Any]:
        if self.output_destination.uses_db:
            return {
                c.name: c.default.arg
                for c in self.schema.columns
                if c.default is not None
            }
        return self.schema.defaults

    def __clean_string(self, data: DataFrame) -> DataFrame:
        """
        Cleans string columns by removing leading/trailing spaces and replacing
        empty strings with NA.
        """
        obj_cols = data.select_dtypes("object").columns.drop(
            "embedding", errors="ignore"
        )
        data.loc[:, obj_cols] = data.loc[:, obj_cols].apply(
            lambda s: (
                s.astype(str)
                .str.strip()
                .replace(["None", "NONE", "nan", "NaN", "<NA>", "null", "NULL", ""], NA)
            )
        )

        all_na_cols = data.columns[data.isna().all()].tolist()

        if all_na_cols:
            print(
                f"[Warning] The following columns contain only NA values: {all_na_cols}"
            )

        return data

    def __validate_schema(self, data: DataFrame) -> None:
        """Validates the CSV schema against the data.
        Checks for duplicate primary keys and null values in not-null columns.
        """
        pk_columns = self.schema.primary_key
        not_null_columns = self.schema.not_null
        if pk_columns and data.duplicated(subset=pk_columns).any():
            raise PrimaryKeyViolationError(pk_columns)
        for col in not_null_columns:
            if data[col].isna().any():
                raise NotNullViolationError(col)

    def __test_connection(self):
        destination = self.output_destination
        if destination.uses_db:
            try:
                with Database(self.connection) as db:
                    db.exec_driver_sql("SELECT 1")
            except (ConnectionError, OSError, RuntimeError) as e:
                raise ConnectionFailedError(self.connection.name, e) from e
        if destination.uses_s3:
            s3 = boto3.client("s3")
            bucket = self.parameters.s3_bucket
            try:
                s3.head_bucket(Bucket=bucket)
            except (ConnectionError, OSError, RuntimeError) as e:
                raise ConnectionFailedError(bucket, e) from e
        if not (destination.uses_db or destination.uses_s3 or destination.uses_email):
            raise NotImplementedError(
                f"Output destination {self.output_destination} not implemented"
            )

    def __email_file_name(self) -> str:
        if hasattr(self.parameters, "s3_key") and self.parameters.s3_key:
            return self.parameters.s3_key.rsplit("/", 1)[-1]
        module_name = self.__class__.__module__.rsplit(".", 1)[-1]
        return f"{module_name}.csv"

    def __send_email(self, data: DataFrame, csv_bytes: bytes) -> None:
        email_sender = EmailSender(
            file_name=self.__email_file_name(),
            category=getattr(self.parameters, "category", None),
            brands=getattr(self.parameters, "brands", None),
            min_date=getattr(self.parameters, "min_date", None),
            max_date=getattr(self.parameters, "max_date", None),
            row_count=data.shape[0],
        )
        email_sender.send(
            recipients=[self.parameters.email_recipients],
            csv_bytes=csv_bytes,
        )

    def __debug(self, data: DataFrame) -> None:
        Path(".output").mkdir(exist_ok=True)
        data.to_csv(Path(".output", f"{self._path}.csv"))
        print(f"[Debug mode: ON] output saved to .output/{self._path}.csv")

    def __log(self, status: Status, error: str | None) -> None:
        with Database(CONNECTIONS["data_warehouse"]) as db:
            db.exec_driver_sql(
                """-- sql
                    INSERT INTO pipeline_logs (pipe_name, manual_trigger, status_id, duration, extracted_rows, dataframes_memory, loaded_rows, uuid, error)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, UUID_TO_BIN(%s), %s)
                 """,
                (
                    self._path,
                    self._manual_trigger,
                    status.value,
                    round(time.time() - self._start_time),
                    self._extracted_rows,
                    int(self._total_memory) / 1024**2,
                    self._loaded_rows,
                    self._uuid,
                    error,
                ),
            )
