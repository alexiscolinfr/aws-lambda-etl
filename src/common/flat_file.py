from dataclasses import dataclass
from typing import Any, Dict, List


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
        self.columns: List[str] = []
        self.defaults: Dict[str, Any] = {}
        self.primary_key: List[str] = []
        self.not_null: List[str] = []

        for col in cols:
            if not isinstance(col, Column):
                raise TypeError(f"Expected Column, got {type(col)}")
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

        # Validate consistency
        unknown_pk = set(self.primary_key) - set(self.columns)
        unknown_nn = set(self.not_null) - set(self.columns)
        if unknown_pk:
            raise ValueError(f"Primary key columns not in 'columns': {unknown_pk}")
        if unknown_nn:
            raise ValueError(f"Not-null columns not in 'columns': {unknown_nn}")
