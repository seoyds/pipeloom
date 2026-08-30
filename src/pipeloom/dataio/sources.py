from __future__ import annotations

import time
from typing import AsyncIterator, Iterable, List, Protocol

from pipeloom.events import RawRecord


class Source(Protocol):
    def get_records(self) -> AsyncIterator[RawRecord]:
        ...


class IterableSource:
    """A Source backed by an in-memory iterable of dicts.

    Useful for tests and small batch jobs. File-format-specific sources
    (JSON/CSV/etc.) are a separate follow-up, not required by the core spec.
    """

    def __init__(self, records: Iterable[dict]) -> None:
        self._records: List[dict] = list(records)

    async def get_records(self) -> AsyncIterator[RawRecord]:
        for value in self._records:
            yield RawRecord(value=value, time=time.time())
