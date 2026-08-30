import csv
import json
from pathlib import Path
from typing import List, Optional, Union

from pipeloom.core import Handler, register_sink_type
from pipeloom.messages import ErrorMessage, Message, Record

__all__ = ["Sink", "row_from_message", "InMemorySink", "JSONLinesSink", "CSVSink"]


class Sink(Handler):
    """Marker base class for a custom, multi-trigger sink Handler you write
    yourself -- register instances via Pipeline.add_sink(your_handler); its
    own @step methods decide what message type(s) it reacts to.

    The built-in sinks below (InMemorySink, JSONLinesSink, CSVSink, and
    pipeloom.dataio.parquet/kafka/db's) are plain classes, not Handlers: each
    has exactly one write() method and no notion of "record" vs "error" of
    its own. Register them via Pipeline.add_sink(sink_type=..., sink_params=
    ..., sink_trigger=...) -- sink_trigger (Record by default) is what
    decides which message type reaches write(). To also persist
    ErrorMessage, add a second, separate add_sink(..., sink_trigger=
    ErrorMessage) call."""


def row_from_message(message: Message) -> dict:
    """Flatten a Record or ErrorMessage into a plain dict -- the row shape
    every built-in sink writes. A sink_trigger of anything else isn't
    supported by these built-ins; write a custom Sink for that."""
    if isinstance(message, Record):
        return dict(message)
    if isinstance(message, ErrorMessage):
        return {
            "message": repr(message.message),
            "exception": str(message.exception),
            "reason": message.reason,
        }
    raise TypeError(f"Don't know how to write a {type(message).__name__} as a row")


class InMemorySink:
    """Collects written messages in memory, unflattened -- each entry is the
    original Record/ErrorMessage/etc. object, not a row. Useful for tests."""

    def __init__(self) -> None:
        self.received: List[Message] = []

    def write(self, message: Message) -> None:
        self.received.append(message)


class JSONLinesSink:
    """Writes rows as JSON Lines to `path`. Call close() when done."""

    def __init__(self, path: Union[str, Path]) -> None:
        self._path = Path(path)
        self._out = self._path.open("w")

    def write(self, message: Message) -> None:
        self._out.write(json.dumps(row_from_message(message)) + "\n")

    def close(self) -> None:
        self._out.close()


class CSVSink:
    """Writes rows as CSV to `path`. The header is taken from the first row's
    keys; every later row must have exactly the same keys -- CSV has no way
    to represent a mid-stream schema change. Call close() when done."""

    def __init__(self, path: Union[str, Path]) -> None:
        self._path = Path(path)
        self._file = self._path.open("w", newline="")
        self._writer: Optional["csv.DictWriter"] = None

    def write(self, message: Message) -> None:
        row = row_from_message(message)
        if self._writer is None:
            self._writer = csv.DictWriter(self._file, fieldnames=list(row.keys()))
            self._writer.writeheader()
        elif set(row.keys()) != set(self._writer.fieldnames):
            raise ValueError(
                f"CSVSink: row fields {set(row.keys())} differ from header "
                f"{set(self._writer.fieldnames)}"
            )
        self._writer.writerow(row)

    def close(self) -> None:
        self._file.close()


register_sink_type("memory", InMemorySink)
register_sink_type("json", JSONLinesSink)
register_sink_type("csv", CSVSink)
