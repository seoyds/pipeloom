"""A pipeline that shrinks and expands as it flows.

Simulates a step that calls an API in bulk and gets one batched response
back (a list of 500 events, say) -- everything downstream needs to work on
individual items, so the batch explodes into records. A later step needs to
call a *different* bulk API, so those individual records get re-grouped into
fixed-size batches again before the final write.

Two directions, two different mechanisms:
- batch -> items ("shrink"): an ordinary @step returning Iterable[Message]
  (or a generator) fans out -- no special class needed, see ExplodeBatch.
- items -> batch ("expand"): needs to accumulate state across many @step
  calls, so it uses pipeloom.core.Batcher -- see RecordBatcher.
"""

import asyncio
import logging
from typing import Iterable, List, Optional

from pipeloom.core import Batcher, Handler, Pipeline, step
from pipeloom.dataio.sinks import Sink
from pipeloom.dataio.sources import IterableSource
from pipeloom.messages import Checkpoint, Message, RawRecord, Record, Shutdown
from pipeloom.pipeline import run
from pipeloom.stages import Stage

logger = logging.getLogger(__name__)


class RecordBatch(Message):
    """A re-grouped batch of already-processed Records, ready for a bulk write."""

    def __init__(self, items: List[Record]) -> None:
        self.items = items

    def __repr__(self) -> str:
        return f"RecordBatch({self.items!r})"


class ExplodeBatch(Handler):
    """Shrink: the source hands us one RawRecord whose value *is* the whole
    batched API response -- explode it into one Record per item so every
    downstream step works on individual rows."""

    @step(Stage.VALIDATE, consume=True)
    def go(self, message: RawRecord) -> Iterable[Record]:
        for item in message.value["items"]:
            yield Record(item)


class UppercaseItem(Handler):
    """Ordinary per-item processing -- runs once per exploded Record."""

    @step(Stage.TRANSFORM, consume=True)
    def go(self, message: Record) -> Record:
        return Record({k: str(v).upper() for k, v in message.items()})


class RecordBatcher(Batcher):
    """Expand: re-groups individual Records into fixed-size RecordBatch
    messages for a downstream bulk write. batch_size=2 here just to make a
    small example produce more than one batch; a real bulk API would use
    whatever its own batch limit is (500, say)."""

    def make_batch(self, items: List[Message]) -> RecordBatch:
        return RecordBatch(list(items))

    @step(Stage.TRANSFORM, consume=True)
    def on_item(self, message: Record) -> Optional[RecordBatch]:
        return self.accumulate(message)

    # End of stream: flush whatever's left so a trailing partial batch is
    # never silently dropped. Checkpoint covers a normal full run; Shutdown
    # is the safety net for an early PipelineStopped, which skips Checkpoint.
    @step(Stage.TRANSFORM, consume=False)
    def on_checkpoint(self, message: Checkpoint) -> Optional[RecordBatch]:
        return self.flush()

    @step(Stage.TRANSFORM, consume=False)
    def on_shutdown(self, message: Shutdown) -> Optional[RecordBatch]:
        return self.flush()


class BulkApiWriter(Sink):
    """Stands in for a bulk-write API call -- one call per RecordBatch,
    not one per row."""

    def __init__(self) -> None:
        self.calls: List[List[Record]] = []

    @step(Stage.LOAD, consume=True)
    def write(self, message: RecordBatch) -> None:
        self.calls.append(message.items)
        logger.info("bulk API call with %d item(s): %s", len(message.items), message.items)


async def main() -> None:
    # One RawRecord standing in for "the batched response an API call
    # returned" -- 5 events, as if this were 500.
    source = IterableSource(
        [
            {
                "items": [
                    {"name": "ada", "role": "engineer"},
                    {"name": "grace", "role": "admiral"},
                    {"name": "alan", "role": "mathematician"},
                    {"name": "margaret", "role": "programmer"},
                    {"name": "katherine", "role": "physicist"},
                ]
            }
        ]
    )

    pipeline = Pipeline()
    pipeline.add_handler(ExplodeBatch())
    pipeline.add_handler(UppercaseItem())
    pipeline.add_handler(RecordBatcher(batch_size=2))

    writer = BulkApiWriter()
    pipeline.add_sink(writer)

    await run(source, pipeline)

    logger.info("Bulk API calls made: %d", len(writer.calls))
    for i, batch in enumerate(writer.calls, start=1):
        logger.info("  call %d: %d item(s)", i, len(batch))
    logger.info(pipeline.format_profile(pipeline.get_profile_data(), blueprint=True))


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-5s %(name)s: %(message)s")
    asyncio.run(main())
