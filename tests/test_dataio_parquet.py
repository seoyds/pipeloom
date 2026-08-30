import pytest

pyarrow = pytest.importorskip("pyarrow")
pq = pytest.importorskip("pyarrow.parquet")

from pipeloom.core import Pipeline
from pipeloom.dataio.parquet import ParquetSink
from pipeloom.messages import Record


@pytest.mark.asyncio
async def test_parquet_sink_writes_records(tmp_path):
    path = tmp_path / "out.parquet"
    pipeline = Pipeline()
    sink = ParquetSink(path)
    pipeline.add_sink(sink)

    await pipeline.process(Record({"name": "ada", "age": 36}))
    await pipeline.process(Record({"name": "grace", "age": 85}))
    sink.close()

    table = pq.read_table(path)
    assert table.to_pylist() == [
        {"name": "ada", "age": 36},
        {"name": "grace", "age": 85},
    ]


@pytest.mark.asyncio
async def test_parquet_sink_flushes_a_full_batch_before_close(tmp_path):
    path = tmp_path / "out.parquet"
    pipeline = Pipeline()
    sink = ParquetSink(path, batch_size=2)
    pipeline.add_sink(sink)

    await pipeline.process(Record({"n": 1}))
    await pipeline.process(Record({"n": 2}))  # triggers an in-flight flush
    assert sink._buffer == []

    await pipeline.process(Record({"n": 3}))
    sink.close()

    table = pq.read_table(path)
    assert table.to_pylist() == [{"n": 1}, {"n": 2}, {"n": 3}]


@pytest.mark.asyncio
async def test_parquet_sink_rejects_a_record_with_different_fields(tmp_path):
    path = tmp_path / "out.parquet"
    pipeline = Pipeline()
    sink = ParquetSink(path)
    pipeline.add_sink(sink)

    await pipeline.process(Record({"name": "ada"}))
    with pytest.raises(ValueError):
        sink.sink_record(Record({"name": "grace", "age": 85}))
    sink.close()
