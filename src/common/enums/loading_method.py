from enum import Enum, auto


class LoadingMethod(Enum):
    """Define how data is loaded in the output database table if it already exists"""

    DROP_INSERT = auto()
    TRUNCATE_INSERT = auto()
    INSERT = auto()
