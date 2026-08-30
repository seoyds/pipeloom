from dataclasses import dataclass, field
from typing import Optional
import time


class Message:
    """Base class for anything flowing through a Pipeline."""


@dataclass
class RawRecord(Message):
    value: dict
    time: float


class Record(Message, dict):
    """A successfully processed record. Behaves like a dict of output fields."""


@dataclass
class ErrorMessage(Message):
    message: Message
    exception: Exception
    reason: Optional[str] = None


@dataclass
class Tick(Message):
    time: float = field(default_factory=time.time)


@dataclass
class Heartbeat(Message):
    time: float = field(default_factory=time.time)
    prev_time: float = field(default_factory=lambda: time.time() - 1.0)


@dataclass
class Checkpoint(Message):
    time: float = field(default_factory=time.time)


@dataclass
class Shutdown(Message):
    time: float = field(default_factory=time.time)
