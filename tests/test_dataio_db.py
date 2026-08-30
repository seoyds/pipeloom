import pytest

sqlalchemy = pytest.importorskip("sqlalchemy")
pytest.importorskip("aiosqlite")

from sqlalchemy import Column, Integer, MetaData, String, Table, select
from sqlalchemy.ext.asyncio import create_async_engine

from pipeloom.core import Pipeline
from pipeloom.dataio.db import DBSink
from pipeloom.messages import Record


async def _make_engine_and_table():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    metadata = MetaData()
    table = Table(
        "people",
        metadata,
        Column("name", String),
        Column("age", Integer),
    )
    async with engine.begin() as conn:
        await conn.run_sync(metadata.create_all)
    return engine, table


@pytest.mark.asyncio
async def test_db_sink_inserts_records():
    engine, table = await _make_engine_and_table()
    pipeline = Pipeline()
    sink = pipeline.add_sink(sink_type="rdb", sink_params={"engine": engine, "table": table})

    await pipeline.process(Record({"name": "ada", "age": 36}))
    await pipeline.process(Record({"name": "grace", "age": 85}))
    await sink.close()

    async with engine.connect() as conn:
        result = await conn.execute(select(table).order_by(table.c.age))
        rows = [dict(r._mapping) for r in result]

    assert rows == [{"name": "ada", "age": 36}, {"name": "grace", "age": 85}]
    await engine.dispose()


@pytest.mark.asyncio
async def test_db_sink_flushes_a_full_batch_before_close():
    engine, table = await _make_engine_and_table()
    pipeline = Pipeline()
    sink = pipeline.add_sink(
        sink_type="rdb", sink_params={"engine": engine, "table": table, "batch_size": 2}
    )

    await pipeline.process(Record({"name": "ada", "age": 36}))
    await pipeline.process(Record({"name": "grace", "age": 85}))  # triggers a flush
    assert sink._buffer == []

    async with engine.connect() as conn:
        result = await conn.execute(select(table))
        rows = [dict(r._mapping) for r in result]
    assert len(rows) == 2

    await sink.close()
    await engine.dispose()


def test_db_sink_requires_either_engine_or_db_type():
    metadata = MetaData()
    table = Table("people", metadata, Column("name", String))
    with pytest.raises(ValueError):
        DBSink(table)


def test_db_sink_rejects_an_unknown_db_type():
    metadata = MetaData()
    table = Table("people", metadata, Column("name", String))
    with pytest.raises(ValueError, match="Unknown db_type"):
        DBSink(table, db_type="snowflake")


@pytest.mark.asyncio
async def test_db_sink_builds_its_own_engine_from_db_type():
    metadata = MetaData()
    table = Table("people", metadata, Column("name", String))
    sink = DBSink(table, db_type="sqlite", database=":memory:")
    async with sink._engine.begin() as conn:
        await conn.run_sync(metadata.create_all)

    await sink.write(Record({"name": "ada"}))
    await sink.close()

    async with sink._engine.connect() as conn:
        result = await conn.execute(select(table))
        rows = [dict(r._mapping) for r in result]
    assert rows == [{"name": "ada"}]
    await sink._engine.dispose()
