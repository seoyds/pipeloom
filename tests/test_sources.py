import pytest

from pipeloom.dataio.sources import CSVSource, IterableSource, JSONLinesSource
from pipeloom.messages import RawRecord


@pytest.mark.asyncio
async def test_iterable_source_yields_raw_records_in_order():
    source = IterableSource([{"a": 1}, {"a": 2}])
    records = [r async for r in source.get_records()]
    assert len(records) == 2
    assert all(isinstance(r, RawRecord) for r in records)
    assert [r.value for r in records] == [{"a": 1}, {"a": 2}]


@pytest.mark.asyncio
async def test_iterable_source_handles_empty_input():
    source = IterableSource([])
    records = [r async for r in source.get_records()]
    assert records == []


@pytest.mark.asyncio
async def test_json_lines_source_yields_raw_records_in_order(tmp_path):
    path = tmp_path / "input.jsonl"
    path.write_text('{"name": "ada"}\n{"name": "grace"}\n')

    source = JSONLinesSource(path)
    records = [r async for r in source.get_records()]

    assert [r.value for r in records] == [{"name": "ada"}, {"name": "grace"}]


@pytest.mark.asyncio
async def test_json_lines_source_skips_blank_lines(tmp_path):
    path = tmp_path / "input.jsonl"
    path.write_text('{"name": "ada"}\n\n{"name": "grace"}\n')

    source = JSONLinesSource(path)
    records = [r async for r in source.get_records()]

    assert [r.value for r in records] == [{"name": "ada"}, {"name": "grace"}]


@pytest.mark.asyncio
async def test_csv_source_yields_raw_records_keyed_by_header(tmp_path):
    path = tmp_path / "input.csv"
    path.write_text("name,role\nada,engineer\ngrace,admiral\n")

    source = CSVSource(path)
    records = [r async for r in source.get_records()]

    assert [r.value for r in records] == [
        {"name": "ada", "role": "engineer"},
        {"name": "grace", "role": "admiral"},
    ]
