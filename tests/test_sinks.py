import pytest

from pipeloom.core import Pipeline
from pipeloom.dataio.sinks import InMemorySink
from pipeloom.events import ErrorMessage, RawRecord, Record


@pytest.mark.asyncio
async def test_in_memory_sink_collects_records():
    pipeline = Pipeline()
    sink = InMemorySink()
    pipeline.add_handler(sink)

    await pipeline.process(Record({"a": 1}))

    assert sink.records == [{"a": 1}]
    assert sink.errors == []


@pytest.mark.asyncio
async def test_in_memory_sink_collects_errors():
    pipeline = Pipeline()
    sink = InMemorySink()
    pipeline.add_handler(sink)

    err = ErrorMessage(message=RawRecord(value={}, time=0.0), exception=ValueError("x"))
    await pipeline.process(err)

    assert sink.errors == [err]
    assert sink.records == []
