"""Essential custom exceptions for the ETL pipeline.

Only exceptions that provide clear domain-specific value over Python built-ins.
For other error cases, use standard Python exceptions with descriptive messages.

### Currently Required Exceptions
- BasePathNotInitializedError: System initialization error
- PrimaryKeyViolationError: Data validation with metadata
- NotNullViolationError: Data validation with context
- InvalidParameterError: Parameter validation with valid options
- MissingDailyDataError: Daily data validation (product prices, inventory, etc.)
- EmptyTableError: Empty table validation for truncated/insert operations
- InvalidDataError: Zero-sum column validation
- TableNotFoundError: Database-specific error
- EmptyRecipientsError: Email recipients list is empty
- SizeLimitExceededError: Payload exceeds a configured size limit
- ResourceNotFoundError: Required file or resource not found
- UnexpectedTypeError: Value has an unexpected type
- MissingColumnError: Required column absent from the data
- ColumnTypeMismatchError: Column contains values of an unexpected type
- UnequalListLengthsError: Multiple list fields have different lengths
- ConnectionFailedError: Connection to a resource cannot be established

"""

from pathlib import Path


# =============================================================================
# SYSTEM ERRORS
# =============================================================================


class BasePathNotInitializedError(RuntimeError):
    """Raised when base_path is accessed before initialization."""

    def __init__(self) -> None:
        super().__init__("base_path not initialized")


# =============================================================================
# DATA VALIDATION ERRORS
# =============================================================================


class PrimaryKeyViolationError(ValueError):
    """Raised when duplicate values are found in primary key columns."""

    def __init__(self, pk_columns: list[str]) -> None:
        super().__init__(f"Duplicate values in primary key: {pk_columns}")
        self.pk_columns = pk_columns


class NotNullViolationError(ValueError):
    """Raised when null values are found in columns that require data."""

    def __init__(self, column: str) -> None:
        super().__init__(f"Nulls in not-null column: {column}")
        self.column = column


class InvalidParameterError(ValueError):
    """Raised when an invalid parameter value is provided."""

    def __init__(
        self, parameter_name: str, value: str, valid_options: list[str] | None = None
    ) -> None:
        message = f"Invalid {parameter_name}: {value}"
        if valid_options:
            message += f". Valid options: {valid_options}"
        super().__init__(message)
        self.parameter_name = parameter_name
        self.value = value
        self.valid_options = valid_options


class MissingDailyDataError(ValueError):
    """Raised when required daily data is missing."""

    def __init__(self, table_name: str, date: str) -> None:
        super().__init__(f"Missing data in table '{table_name}' for date: {date}")
        self.table_name = table_name
        self.date = date


class EmptyTableError(ValueError):
    """Raised when a required table is empty when it should contain data."""

    def __init__(self, table_name: str, reason: str | None = None) -> None:
        message = f"The '{table_name}' table is currently empty."
        if reason:
            message += f" {reason}"
        super().__init__(message)
        self.table_name = table_name
        self.reason = reason


class InvalidDataError(ValueError):
    """Raised when columns have no valid values (zero sum)."""

    def __init__(self, date: str, zero_sum_columns: list[str]) -> None:
        super().__init__(
            f"Invalid data on {date}: "
            f"the following columns have no valid values (sum == 0): {zero_sum_columns}"
        )
        self.date = date
        self.zero_sum_columns = zero_sum_columns


class MissingColumnError(ValueError):
    """Raised when a required column is absent from the data."""

    def __init__(self, column: str) -> None:
        super().__init__(f"Missing required column '{column}'")
        self.column = column


class UnequalListLengthsError(ValueError):
    """Raised when multiple list fields in a JSON object have different lengths."""

    def __init__(self, lengths: dict) -> None:
        super().__init__(f"All list fields must have the same length. Got: {lengths}")
        self.lengths = lengths


# =============================================================================
# TYPE ERRORS
# =============================================================================


class UnexpectedTypeError(TypeError):
    """Raised when a value has an unexpected type."""

    def __init__(self, expected: str, actual: type) -> None:
        super().__init__(f"Expected {expected}, got {actual}")
        self.expected = expected
        self.actual = actual


class ColumnTypeMismatchError(ValueError):
    """Raised when a column contains values of an unexpected type."""

    def __init__(self, column: str, expected: type) -> None:
        super().__init__(f"Column '{column}' contains values not of type {expected}")
        self.column = column
        self.expected = expected


# =============================================================================
# DATABASE ERRORS
# =============================================================================


class TableNotFoundError(LookupError):
    """Raised when a required table is not found in the database."""

    def __init__(self, table_name: str) -> None:
        super().__init__(f"Table {table_name} does not exist")
        self.table_name = table_name


class ConnectionFailedError(ConnectionError):
    """Raised when a connection to a resource cannot be established."""

    def __init__(self, resource: str, cause: Exception) -> None:
        super().__init__(f"Connection failed for {resource}: {cause}")
        self.resource = resource
        self.cause = cause


# =============================================================================
# IO ERRORS
# =============================================================================


class ResourceNotFoundError(FileNotFoundError):
    """Raised when a required file or resource is not found."""

    def __init__(self, path: str | Path) -> None:
        super().__init__(f"Resource not found: {path}")
        self.path = path


class EmptyRecipientsError(ValueError):
    """Raised when the email recipients list is empty."""

    def __init__(self) -> None:
        super().__init__("Email recipients list is empty")


class SizeLimitExceededError(ValueError):
    """Raised when a payload exceeds a configured size limit."""

    def __init__(self, actual: int, limit: int) -> None:
        super().__init__(f"Size limit exceeded: {actual} bytes > {limit} bytes")
        self.actual = actual
        self.limit = limit
