from dataclasses import dataclass
from typing import Any

from common.exceptions import UnexpectedTypeError


@dataclass
class Column:
    """
    Defines a column for a flat file schema, similar to SQLAlchemy Column.

    Attributes:
        name: Column name.
        default: Default value when missing.
        primary_key: Whether this column is part of the primary key.
        nullable: Whether this column allows null values.
    """

    name: str
    default: Any = None
    primary_key: bool = False
    nullable: bool = True

    def __post_init__(self):
        if self.primary_key:
            self.nullable = False


class FlatFile:
    """
    Represents a flat file schema defined via Column objects.
    """

    def __init__(self, *cols: Column):
        self.columns: list[str] = []
        self.defaults: dict[str, Any] = {}
        self.primary_key: list[str] = []
        self.not_null: list[str] = []

        for col in cols:
            if not isinstance(col, Column):
                raise UnexpectedTypeError("Column", type(col))
            # Record column name
            self.columns.append(col.name)
            # Default value
            if col.default is not None:
                self.defaults[col.name] = col.default
            # Primary key
            if col.primary_key:
                self.primary_key.append(col.name)
            # Not-null constraint
            if not col.nullable:
                self.not_null.append(col.name)
