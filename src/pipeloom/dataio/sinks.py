from typing import List

from pipeloom.core import Handler, step
from pipeloom.events import ErrorMessage, Record
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
