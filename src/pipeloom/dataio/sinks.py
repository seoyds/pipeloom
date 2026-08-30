import csv
import json
from pathlib import Path
from typing import List, Optional, Union

from pipeloom.core import Handler, step
from pipeloom.messages import ErrorMessage, Record
from pipeloom.stages import Stage


class Sink(Handler):
    """Marker base class for Handlers whose job is to consume final output."""


class InMemorySink(Sink):
    """Collects written Records and ErrorMessages in memory. Useful for tests."""

    def __init__(self) -> None:
        self.records: List[dict] = []
        self.errors: List[ErrorMessage] = []

    @step(Stage.LOAD, consume=True)
    def sink_record(self, message: Record) -> None:
        self.records.append(dict(message))

    @step(Stage.LOAD, consume=True)
    def sink_error(self, message: ErrorMessage) -> None:
        self.errors.append(message)


class JSONLinesSink(Sink):
    """Writes successful Records as JSON Lines to `path`, and ErrorMessages as
    JSON Lines to `error_path` (defaults to `path` + ".errors", opened lazily
    -- a run with no errors never creates it). Call close() when done."""

    def __init__(self, path: Union[str, Path], error_path: Optional[Union[str, Path]] = None) -> None:
        self._path = Path(path)
        self._error_path = Path(error_path) if error_path is not None else self._path.with_name(self._path.name + ".errors")
        self._out = self._path.open("w")
        self._err = None

    @step(Stage.LOAD, consume=True)
    def sink_record(self, message: Record) -> None:
        self._out.write(json.dumps(dict(message)) + "\n")

    @step(Stage.LOAD, consume=True)
    def sink_error(self, message: ErrorMessage) -> None:
        if self._err is None:
            self._err = self._error_path.open("w")
        self._err.write(
            json.dumps(
                {
                    "message": repr(message.message),
                    "exception": str(message.exception),
                    "reason": message.reason,
                }
            )
            + "\n"
        )

    def close(self) -> None:
        self._out.close()
        if self._err is not None:
            self._err.close()


class CSVSink(Sink):
    """Writes successful Records as CSV rows to `path`. The header is taken
    from the first Record's keys; every later Record must have exactly the
    same keys -- CSV has no way to represent a mid-stream schema change.
    Route ErrorMessages elsewhere (e.g. JSONLinesSink). Call close() when done.
    """

    def __init__(self, path: Union[str, Path]) -> None:
        self._path = Path(path)
        self._file = self._path.open("w", newline="")
        self._writer: Optional["csv.DictWriter"] = None

    @step(Stage.LOAD, consume=True)
    def sink_record(self, message: Record) -> None:
        row = dict(message)
        if self._writer is None:
            self._writer = csv.DictWriter(self._file, fieldnames=list(row.keys()))
            self._writer.writeheader()
        elif set(row.keys()) != set(self._writer.fieldnames):
            raise ValueError(
                f"CSVSink: record fields {set(row.keys())} differ from header "
                f"{set(self._writer.fieldnames)}"
            )
        self._writer.writerow(row)

    def close(self) -> None:
        self._file.close()
