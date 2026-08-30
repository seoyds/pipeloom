import pytest

from pipeloom.dataio.sources import IterableSource
from pipeloom.events import RawRecord


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
