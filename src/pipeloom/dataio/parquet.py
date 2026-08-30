"""Optional Parquet sink -- requires the `parquet` extra (pyarrow).

Nothing in pipeloom.dataio.sinks/sources imports this module; import it
explicitly to use ParquetSink.
"""

from pathlib import Path
from typing import List, Optional, Union

import pyarrow as pa
import pyarrow.parquet as pq

from pipeloom.core import step
from pipeloom.dataio.sinks import Sink
from pipeloom.messages import Record
from pipeloom.stages import Stage


class ParquetSink(Sink):
    """Writes successful Records to a Parquet file at `path`.

    The schema is inferred from the first Record's keys/value types; every
    later Record must have exactly the same keys. Buffers up to `batch_size`
    records before writing a row group. Call close() to flush the rest and
    finish the file -- an unclosed file is not valid Parquet. Route
    ErrorMessages elsewhere (e.g. JSONLinesSink).
    """

    def __init__(self, path: Union[str, Path], batch_size: int = 1000) -> None:
        self._path = Path(path)
        self._batch_size = batch_size
        self._buffer: List[dict] = []
        self._writer: Optional[pq.ParquetWriter] = None
        self._schema: Optional[pa.Schema] = None

    @step(Stage.LOAD, consume=True)
    def sink_record(self, message: Record) -> None:
        row = dict(message)
        if self._schema is None:
            self._schema = pa.Table.from_pylist([row]).schema
            self._writer = pq.ParquetWriter(str(self._path), self._schema)
        elif set(row.keys()) != set(self._schema.names):
            raise ValueError(
                f"ParquetSink: record fields {set(row.keys())} differ from schema "
                f"{set(self._schema.names)}"
            )
        self._buffer.append(row)
        if len(self._buffer) >= self._batch_size:
            self._flush()

    def _flush(self) -> None:
        if not self._buffer:
            return
        table = pa.Table.from_pylist(self._buffer, schema=self._schema)
        self._writer.write_table(table)
        self._buffer = []

    def close(self) -> None:
        self._flush()
        if self._writer is not None:
            self._writer.close()
