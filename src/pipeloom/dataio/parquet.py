"""Optional Parquet sink -- requires the `parquet` extra (pyarrow).

Nothing in pipeloom.dataio.sinks/sources imports this module; import it
explicitly to register sink_type="parquet".
"""

from pathlib import Path
from typing import List, Optional, Union

import pyarrow as pa
import pyarrow.parquet as pq

from pipeloom.core import register_sink_type
from pipeloom.dataio.sinks import row_from_message
from pipeloom.messages import Message

__all__ = ["ParquetSink"]


class ParquetSink:
    """Writes rows to a Parquet file at `path`.

    The schema is inferred from the first row's keys/value types; every
    later row must have exactly the same keys. Buffers up to `batch_size`
    rows before writing a row group. Call close() to flush the rest and
    finish the file -- an unclosed file is not valid Parquet.
    """

    def __init__(self, path: Union[str, Path], batch_size: int = 1000) -> None:
        self._path = Path(path)
        self._batch_size = batch_size
        self._buffer: List[dict] = []
        self._writer: Optional[pq.ParquetWriter] = None
        self._schema: Optional[pa.Schema] = None

    def write(self, message: Message) -> None:
        row = row_from_message(message)
        if self._schema is None:
            self._schema = pa.Table.from_pylist([row]).schema
            self._writer = pq.ParquetWriter(str(self._path), self._schema)
        elif set(row.keys()) != set(self._schema.names):
            raise ValueError(
                f"ParquetSink: row fields {set(row.keys())} differ from schema "
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


register_sink_type("parquet", ParquetSink)
