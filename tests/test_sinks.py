import csv
import json

import pytest

from pipeloom.core import Pipeline
from pipeloom.dataio.sinks import CSVSink, InMemorySink, JSONLinesSink
from pipeloom.messages import ErrorMessage, RawRecord, Record


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


@pytest.mark.asyncio
async def test_json_lines_sink_writes_records_as_json_lines(tmp_path):
    path = tmp_path / "out.jsonl"
    pipeline = Pipeline()
    sink = JSONLinesSink(path)
    pipeline.add_handler(sink)

    await pipeline.process(Record({"name": "ADA"}))
    await pipeline.process(Record({"name": "GRACE"}))
    sink.close()

    lines = path.read_text().splitlines()
    assert [json.loads(line) for line in lines] == [{"name": "ADA"}, {"name": "GRACE"}]


@pytest.mark.asyncio
async def test_json_lines_sink_writes_errors_to_a_lazily_opened_error_file(tmp_path):
    path = tmp_path / "out.jsonl"
    error_path = tmp_path / "out.jsonl.errors"
    pipeline = Pipeline()
    sink = JSONLinesSink(path)
    pipeline.add_handler(sink)

    assert not error_path.exists()

    err = ErrorMessage(message=RawRecord(value={}, time=0.0), exception=ValueError("boom"))
    await pipeline.process(err)
    sink.close()

    assert error_path.exists()
    logged = json.loads(error_path.read_text().splitlines()[0])
    assert logged["exception"] == "boom"


@pytest.mark.asyncio
async def test_csv_sink_writes_a_header_from_the_first_record(tmp_path):
    path = tmp_path / "out.csv"
    pipeline = Pipeline()
    sink = CSVSink(path)
    pipeline.add_handler(sink)

    await pipeline.process(Record({"name": "ada", "role": "engineer"}))
    await pipeline.process(Record({"name": "grace", "role": "admiral"}))
    sink.close()

    with path.open(newline="") as f:
        rows = list(csv.DictReader(f))
    assert rows == [
        {"name": "ada", "role": "engineer"},
        {"name": "grace", "role": "admiral"},
    ]


@pytest.mark.asyncio
async def test_csv_sink_rejects_a_record_with_different_fields(tmp_path):
    path = tmp_path / "out.csv"
    pipeline = Pipeline()
    sink = CSVSink(path)
    pipeline.add_handler(sink)

    await pipeline.process(Record({"name": "ada"}))
    with pytest.raises(ValueError):
        sink.sink_record(Record({"name": "grace", "role": "admiral"}))
    sink.close()
