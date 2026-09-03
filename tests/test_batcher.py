from typing import List, Optional

import pytest

from pipeloom.core import Batcher, Handler, Pipeline, step
from pipeloom.dataio.sinks import InMemorySink  # noqa: F401 -- registers sink_type="memory"
from pipeloom.dataio.sources import IterableSource
from pipeloom.messages import Checkpoint, Message, RawRecord, Record, Shutdown
from pipeloom.pipeline import run
from pipeloom.stages import Stage


class RecordBatch(Message):
    def __init__(self, items: List[Record]) -> None:
        self.items = items


class ExampleBatcher(Batcher):
    def make_batch(self, items: List[Message]) -> RecordBatch:
        return RecordBatch(items)


def test_batch_size_must_be_at_least_one():
    with pytest.raises(ValueError):
        ExampleBatcher(batch_size=0)


def test_accumulate_returns_none_until_batch_size_reached():
    batcher = ExampleBatcher(batch_size=3)
    assert batcher.accumulate(Record({"n": 1})) is None
    assert batcher.accumulate(Record({"n": 2})) is None


def test_accumulate_returns_batch_when_size_reached():
    batcher = ExampleBatcher(batch_size=2)
    assert batcher.accumulate(Record({"n": 1})) is None
    batch = batcher.accumulate(Record({"n": 2}))
    assert isinstance(batch, RecordBatch)
    assert batch.items == [Record({"n": 1}), Record({"n": 2})]


def test_buffer_starts_fresh_after_a_full_batch():
    batcher = ExampleBatcher(batch_size=2)
    batcher.accumulate(Record({"n": 1}))
    batcher.accumulate(Record({"n": 2}))
    assert batcher.accumulate(Record({"n": 3})) is None


def test_flush_returns_none_when_buffer_empty():
    batcher = ExampleBatcher(batch_size=2)
    assert batcher.flush() is None


def test_flush_returns_partial_batch_and_clears_buffer():
    batcher = ExampleBatcher(batch_size=5)
    batcher.accumulate(Record({"n": 1}))
    batch = batcher.flush()
    assert isinstance(batch, RecordBatch)
    assert batch.items == [Record({"n": 1})]
    assert batcher.flush() is None


def test_make_batch_is_not_implemented_by_default():
    batcher = Batcher(batch_size=1)
    with pytest.raises(NotImplementedError):
        batcher.accumulate(Record({"n": 1}))


class ExplodeBatch(Handler):
    """Shrink: one ApiBatchResult-shaped RawRecord becomes N individual Records."""

    @step(Stage.TRANSFORM, consume=True)
    def go(self, message: RawRecord) -> "list[Record]":
        return [Record(item) for item in message.value["items"]]


class RecordBatcher(Batcher):
    """Expand: individual Records re-grouped into fixed-size RecordBatch messages."""

    def __init__(self, batch_size: int) -> None:
        super().__init__(batch_size)

    def make_batch(self, items: List[Message]) -> RecordBatch:
        return RecordBatch(list(items))

    @step(Stage.TRANSFORM, consume=True)
    def on_item(self, message: Record) -> Optional[RecordBatch]:
        return self.accumulate(message)

    @step(Stage.TRANSFORM, consume=False)
    def on_checkpoint(self, message: Checkpoint) -> Optional[RecordBatch]:
        return self.flush()

    @step(Stage.TRANSFORM, consume=False)
    def on_shutdown(self, message: Shutdown) -> Optional[RecordBatch]:
        return self.flush()


@pytest.mark.asyncio
async def test_batch_explodes_to_items_then_rebatches_downstream():
    source = IterableSource([{"items": [{"n": 1}, {"n": 2}, {"n": 3}, {"n": 4}, {"n": 5}]}])
    pipeline = Pipeline()
    batches_sink = pipeline.add_sink(sink_type="memory", sink_trigger=RecordBatch)
    pipeline.add_handler(ExplodeBatch())
    pipeline.add_handler(RecordBatcher(batch_size=2))

    await run(source, pipeline)

    received: List[RecordBatch] = batches_sink.received
    assert [b.items for b in received] == [
        [Record({"n": 1}), Record({"n": 2})],
        [Record({"n": 3}), Record({"n": 4})],
        [Record({"n": 5})],  # trailing partial batch flushed on Checkpoint
    ]
