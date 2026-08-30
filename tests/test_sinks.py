import csv
import json

import pytest

from pipeloom.core import Pipeline
from pipeloom.dataio.sinks import CSVSink, InMemorySink, JSONLinesSink, row_from_message
from pipeloom.messages import ErrorMessage, RawRecord, Record


def test_row_from_message_flattens_a_record():
    assert row_from_message(Record({"a": 1})) == {"a": 1}


def test_row_from_message_flattens_an_error_message():
    err = ErrorMessage(message=RawRecord(value={}, time=0.0), exception=ValueError("boom"), reason="bad_input")
    row = row_from_message(err)
    assert row["exception"] == "boom"
    assert row["reason"] == "bad_input"


def test_row_from_message_rejects_an_unsupported_message_type():
    with pytest.raises(TypeError):
        row_from_message(RawRecord(value={}, time=0.0))


@pytest.mark.asyncio
async def test_in_memory_sink_collects_records_via_add_sink():
    pipeline = Pipeline()
    sink = pipeline.add_sink(sink_type="memory", sink_trigger=Record)

    await pipeline.process(Record({"a": 1}))

    assert sink.received == [{"a": 1}]


@pytest.mark.asyncio
async def test_in_memory_sink_collects_errors_via_a_separate_add_sink_call():
    pipeline = Pipeline()
    records_sink = pipeline.add_sink(sink_type="memory", sink_trigger=Record)
    errors_sink = pipeline.add_sink(sink_type="memory", sink_trigger=ErrorMessage)

    err = ErrorMessage(message=RawRecord(value={}, time=0.0), exception=ValueError("x"))
    await pipeline.process(err)

    assert errors_sink.received == [err]
    assert records_sink.received == []


@pytest.mark.asyncio
async def test_json_lines_sink_writes_rows_as_json_lines(tmp_path):
    path = tmp_path / "out.jsonl"
    pipeline = Pipeline()
    sink = pipeline.add_sink(sink_type="json", sink_params={"path": path}, sink_trigger=Record)

    await pipeline.process(Record({"name": "ADA"}))
    await pipeline.process(Record({"name": "GRACE"}))
    sink.close()

    lines = path.read_text().splitlines()
    assert [json.loads(line) for line in lines] == [{"name": "ADA"}, {"name": "GRACE"}]


@pytest.mark.asyncio
async def test_json_lines_sink_can_be_registered_a_second_time_for_errors(tmp_path):
    out_path = tmp_path / "out.jsonl"
    error_path = tmp_path / "errors.jsonl"
    pipeline = Pipeline()
    records_sink = pipeline.add_sink(sink_type="json", sink_params={"path": out_path}, sink_trigger=Record)
    errors_sink = pipeline.add_sink(
        sink_type="json", sink_params={"path": error_path}, sink_trigger=ErrorMessage
    )

    err = ErrorMessage(message=RawRecord(value={}, time=0.0), exception=ValueError("boom"))
    await pipeline.process(err)
    records_sink.close()
    errors_sink.close()

    logged = json.loads(error_path.read_text().splitlines()[0])
    assert logged["exception"] == "boom"
    assert out_path.read_text() == ""


@pytest.mark.asyncio
async def test_csv_sink_writes_a_header_from_the_first_record(tmp_path):
    path = tmp_path / "out.csv"
    pipeline = Pipeline()
    sink = pipeline.add_sink(sink_type="csv", sink_params={"path": path}, sink_trigger=Record)

    await pipeline.process(Record({"name": "ada", "role": "engineer"}))
    await pipeline.process(Record({"name": "grace", "role": "admiral"}))
    sink.close()

    with path.open(newline="") as f:
        rows = list(csv.DictReader(f))
    assert rows == [
        {"name": "ada", "role": "engineer"},
        {"name": "grace", "role": "admiral"},
    ]


def test_csv_sink_rejects_a_row_with_different_fields(tmp_path):
    path = tmp_path / "out.csv"
    sink = CSVSink(path)

    sink.write(Record({"name": "ada"}))
    with pytest.raises(ValueError):
        sink.write(Record({"name": "grace", "role": "admiral"}))
    sink.close()


def test_in_memory_sink_write_is_a_single_method_with_no_error_handling_baked_in():
    sink = InMemorySink()
    assert hasattr(sink, "write")
    assert not hasattr(sink, "sink_record")
    assert not hasattr(sink, "sink_error")
