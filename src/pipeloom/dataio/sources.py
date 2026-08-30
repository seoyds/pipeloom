from __future__ import annotations

import csv
import json
import time
from pathlib import Path
from typing import AsyncIterator, Iterable, List, Protocol, Union

from pipeloom.messages import RawRecord


class Source(Protocol):
    def get_records(self) -> AsyncIterator[RawRecord]:
        ...


class IterableSource:
    """A Source backed by an in-memory iterable of dicts.

    Useful for tests and small batch jobs.
    """

    def __init__(self, records: Iterable[dict]) -> None:
        self._records: List[dict] = list(records)

    async def get_records(self) -> AsyncIterator[RawRecord]:
        for value in self._records:
            yield RawRecord(value=value, time=time.time())


class JSONLinesSource:
    """A Source backed by a JSON Lines file: one JSON object per line."""

    def __init__(self, path: Union[str, Path]) -> None:
        self._path = Path(path)

    async def get_records(self) -> AsyncIterator[RawRecord]:
        with self._path.open() as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                yield RawRecord(value=json.loads(line), time=time.time())


class CSVSource:
    """A Source backed by a CSV file; each row becomes a RawRecord's value
    (via csv.DictReader, keyed by the header row)."""

    def __init__(self, path: Union[str, Path]) -> None:
        self._path = Path(path)

    async def get_records(self) -> AsyncIterator[RawRecord]:
        with self._path.open(newline="") as f:
            for row in csv.DictReader(f):
                yield RawRecord(value=dict(row), time=time.time())
