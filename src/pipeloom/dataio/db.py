"""Optional database sink -- requires the `db` extra (SQLAlchemy's asyncio
engine) plus a driver for your database (e.g. asyncpg for Postgres,
aiomysql for MySQL, aiosqlite for SQLite).

Nothing in pipeloom.dataio.sinks/sources imports this module; import it
explicitly to use DBSink.
"""

from typing import List

from sqlalchemy import Table
from sqlalchemy.ext.asyncio import AsyncEngine

from pipeloom.core import step
from pipeloom.dataio.sinks import Sink
from pipeloom.messages import Record
from pipeloom.stages import Stage


class DBSink(Sink):
    """Inserts successful Records into `table` via a SQLAlchemy async engine.

    Buffers up to `batch_size` records and inserts them in one executemany
    statement. `table` must already exist -- DBSink never creates or
    migrates schema. close() is async; await it to flush any buffered
    records before shutdown.
    """

    def __init__(self, engine: AsyncEngine, table: Table, batch_size: int = 100) -> None:
        self._engine = engine
        self._table = table
        self._batch_size = batch_size
        self._buffer: List[dict] = []

    @step(Stage.LOAD, consume=True)
    async def sink_record(self, message: Record) -> None:
        self._buffer.append(dict(message))
        if len(self._buffer) >= self._batch_size:
            await self._flush()

    async def _flush(self) -> None:
        if not self._buffer:
            return
        rows, self._buffer = self._buffer, []
        async with self._engine.begin() as conn:
            await conn.execute(self._table.insert(), rows)

    async def close(self) -> None:
        await self._flush()
