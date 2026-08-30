"""Optional database sink -- requires the `db` extra (SQLAlchemy's asyncio
engine) plus a driver for your database (asyncpg for Postgres, aiomysql for
MySQL, aiosqlite for SQLite).

Nothing in pipeloom.dataio.sinks/sources imports this module; import it
explicitly to register sink_type="rdb".
"""

from typing import Any, Dict, List, Optional

from sqlalchemy import Table
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from pipeloom.core import register_sink_type
from pipeloom.dataio.sinks import row_from_message
from pipeloom.messages import Message

__all__ = ["DBSink"]

# Async-native drivers only -- SQLAlchemy's AsyncEngine requires an asyncio
# DBAPI. A database without one (e.g. Snowflake, whose connector is sync-only
# and has no asyncio SQLAlchemy dialect) isn't in this table: build the
# engine yourself and pass it as `engine`, or write a custom Sink Handler and
# register it via Pipeline.add_sink(handler) instead of sink_type="rdb".
_URL_TEMPLATES: Dict[str, str] = {
    "postgres": "postgresql+asyncpg://{user}:{password}@{host}:{port}/{database}",
    "postgresql": "postgresql+asyncpg://{user}:{password}@{host}:{port}/{database}",
    "mysql": "mysql+aiomysql://{user}:{password}@{host}:{port}/{database}",
    "sqlite": "sqlite+aiosqlite:///{database}",
}


def _build_engine(db_type: str, **fields: Any) -> AsyncEngine:
    try:
        template = _URL_TEMPLATES[db_type]
    except KeyError:
        raise ValueError(
            f"Unknown db_type {db_type!r}. Supported: {sorted(_URL_TEMPLATES)}. For a "
            "database without a native asyncio driver (e.g. Snowflake), build the "
            "engine yourself and pass it as sink_params={'engine': ...}."
        ) from None
    return create_async_engine(template.format(**fields))


class DBSink:
    """Inserts rows into `table` via a SQLAlchemy async engine.

    Configure with EITHER `engine` (a pre-built AsyncEngine, e.g. for a
    database without a native asyncio driver, wired through your own
    adapter) OR `db_type` plus its connection fields ("postgres"/"mysql"/
    "sqlite", host, port, user, password, database) -- pipeloom builds the
    engine for you from the latter. `table` must already exist -- DBSink
    never creates or migrates schema. Buffers up to `batch_size` rows and
    inserts them in one executemany statement; close() is async, await it
    to flush the rest.
    """

    def __init__(
        self,
        table: Table,
        engine: Optional[AsyncEngine] = None,
        db_type: Optional[str] = None,
        batch_size: int = 100,
        **connection_fields: Any,
    ) -> None:
        if engine is None:
            if db_type is None:
                raise ValueError("DBSink requires either engine or db_type")
            engine = _build_engine(db_type, **connection_fields)
        self._engine = engine
        self._table = table
        self._batch_size = batch_size
        self._buffer: List[dict] = []

    async def write(self, message: Message) -> None:
        self._buffer.append(row_from_message(message))
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


register_sink_type("rdb", DBSink)
