import json
from collections.abc import Callable, Iterable
from pathlib import Path

import pandas as pd

from common.exceptions import (
    ColumnTypeMismatchError,
    MissingColumnError,
    ResourceNotFoundError,
    UnequalListLengthsError,
)


class JsonLoader:
    """
    Universal JSON → pandas DataFrame loader.

    Supported structures:
    - list of dicts → one row per dict
    - dict with:
        • one list field → explode this list
        • multiple list fields → all lists must have the same length
    - scalars or lists of scalars → stored in a single 'value' column
    """

    def __init__(
        self,
        sources: str | Path | Iterable[str | Path] | dict | list,
        *,
        flatten_dicts: bool = True,
        deduplicate: bool = True,
        schema: dict[str, Callable] | None = None,
    ):
        """
        Parameters
        ----------
        sources : path(s), dict or list
            JSON file, directory, or in-memory object
        flatten_dicts : bool
            Flatten nested dicts using pandas.json_normalize
        deduplicate : bool
            Drop duplicate rows
        schema : dict[str, callable], optional
            Lightweight validation rules, e.g.:
            {"entity_id": int, "model_config_id": int}
        """
        self.sources = sources
        self.flatten_dicts = flatten_dicts
        self.deduplicate = deduplicate
        self.schema = schema

    def __call__(self) -> pd.DataFrame:
        """Allow loader() syntax."""
        return self.load()

    @classmethod
    def from_path(
        cls,
        paths: str | Path | Iterable[str | Path],
        **kwargs,
    ) -> "JsonLoader":
        """Create a loader from file or directory path(s)."""
        return cls(paths, **kwargs)

    @classmethod
    def from_object(
        cls,
        obj: dict | list,
        **kwargs,
    ) -> "JsonLoader":
        """Create a loader from an in-memory JSON object."""
        return cls(obj, **kwargs)

    def load(self) -> pd.DataFrame:
        """Load the JSON sources into a pandas DataFrame."""
        rows: list[dict] = []

        for parsed in self._read_sources():
            rows.extend(self._parsed_to_rows(parsed))

        if not rows:
            return pd.DataFrame()

        dataframe = (
            pd.json_normalize(rows) if self.flatten_dicts else pd.DataFrame(rows)
        )

        if self.schema:
            self.validate(dataframe)

        if self.deduplicate:
            dataframe = dataframe.drop_duplicates().reset_index(drop=True)

        return dataframe

    def _parsed_to_rows(self, parsed) -> list[dict]:
        """Convert a parsed JSON object to a list of row dicts."""
        # list case
        if isinstance(parsed, list):
            if all(isinstance(x, dict) for x in parsed):
                return list(parsed)
            return [x if isinstance(x, dict) else {"value": x} for x in parsed]

        # dict case
        if isinstance(parsed, dict):
            list_keys = [k for k, v in parsed.items() if isinstance(v, list)]

            # no list → single row
            if not list_keys:
                return [parsed]

            # single list → explode
            if len(list_keys) == 1:
                key = list_keys[0]
                base = {k: v for k, v in parsed.items() if k != key}
                return [{**base, key: v} for v in parsed[key]]

            # multiple lists → strict zip (same length required)
            lengths = {k: len(parsed[k]) for k in list_keys}
            if len(set(lengths.values())) != 1:
                raise UnequalListLengthsError(lengths)

            base = {k: v for k, v in parsed.items() if k not in list_keys}
            result = []
            for i in range(next(iter(lengths.values()))):
                row = dict(base)
                for k in list_keys:
                    row[k] = parsed[k][i]
                result.append(row)
            return result

        # scalar fallback
        return [{"value": parsed}]

    def validate(self, df: pd.DataFrame) -> None:
        """
        Lightweight schema validation.

        schema example:
        {
            "entity_id": int,
            "model_config_id": int,
        }
        """
        for column, expected in self.schema.items():
            if column not in df.columns:
                raise MissingColumnError(column)

            if expected is None:
                continue

            if (
                not df[column]
                .dropna()
                .map(lambda x, expected=expected: isinstance(x, expected))
                .all()
            ):
                raise ColumnTypeMismatchError(column, expected)

    def _read_sources(self):
        """Yield parsed JSON objects from files, directories, or in-memory data."""
        sources = self.sources

        if self._is_single_path(sources):
            sources = [sources]

        if isinstance(sources, dict):
            yield sources
            return

        if isinstance(sources, list):
            if not sources:
                return
            if self._is_in_memory_json_list(sources):
                yield sources
                return

        yield from self._read_from_paths(sources)

    def _is_single_path(self, sources):
        return isinstance(sources, str | Path)

    def _is_in_memory_json_list(self, sources):
        return any(isinstance(x, dict | list) for x in sources)

    def _read_from_paths(self, sources):
        for s in sources:
            p = Path(s)
            if not p.exists():
                raise ResourceNotFoundError(p)

            if p.is_dir():
                yield from self._read_from_directory(p)
                continue

            text = p.read_text(encoding="utf-8").strip()
            if not text:
                continue

            parsed = self._try_load_json(text, p)
            if parsed is not None:
                yield parsed

    def _read_from_directory(self, directory):
        for f in (
            sorted(directory.glob("*.json"))
            + sorted(directory.glob("*.jsonl"))
            + sorted(directory.glob("*.ndjson"))
        ):
            yield from (
                JsonLoader(
                    f,
                    flatten_dicts=self.flatten_dicts,
                    deduplicate=self.deduplicate,
                    schema=self.schema,
                )
                .load()
                .to_dict(orient="records")
            )

    def _try_load_json(self, text, path):
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            # ndjson fallback (one JSON document per line)
            return self._try_load_ndjson(path)

    def _try_load_ndjson(self, path):
        """Parse ndjson file (one JSON document per line)."""
        results = []
        with path.open("r", encoding="utf-8") as fh:
            for line in fh:
                stripped_line = line.strip()
                if stripped_line:
                    results.append(json.loads(stripped_line))
        return results if results else None
