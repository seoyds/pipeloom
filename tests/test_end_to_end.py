import asyncio

import pytest

from pipeloom.core import Handler, Pipeline, step
from pipeloom.dataio.sinks import InMemorySink
from pipeloom.dataio.sources import IterableSource, Source
from pipeloom.messages import Checkpoint, RawRecord, Record, Shutdown, Tick
from pipeloom.exceptions import PipelineStopped
from pipeloom.pipeline import run
from pipeloom.stages import Stage


class UppercaseTransform(Handler):
    @step(Stage.TRANSFORM, consume=True)
    def go(self, message: RawRecord) -> Record:
        return Record({k: str(v).upper() for k, v in message.value.items()})


class FailOnEmpty(Handler):
    @step(Stage.VALIDATE, consume=True)
    def go(self, message: RawRecord) -> RawRecord:
        if not message.value:
            raise ValueError("empty record")
        return message


@pytest.mark.asyncio
async def test_run_processes_every_record_from_source_to_sink():
    source = IterableSource([{"name": "ada"}, {"name": "grace"}])
    pipeline = Pipeline()
    sink = InMemorySink()
    pipeline.add_handler(FailOnEmpty())
    pipeline.add_handler(UppercaseTransform())
    pipeline.add_handler(sink)

    await run(source, pipeline)

    assert sink.records == [{"name": "ADA"}, {"name": "GRACE"}]
    assert sink.errors == []


@pytest.mark.asyncio
async def test_run_routes_a_failing_record_to_the_error_sink_and_continues():
    source = IterableSource([{}, {"name": "grace"}])
    pipeline = Pipeline()
    sink = InMemorySink()
    pipeline.add_handler(FailOnEmpty())
    pipeline.add_handler(UppercaseTransform())
    pipeline.add_handler(sink)

    await run(source, pipeline)

    assert sink.records == [{"name": "GRACE"}]
    assert len(sink.errors) == 1
    assert isinstance(sink.errors[0].exception, ValueError)


class StopOnSentinel(Handler):
    @step(Stage.VALIDATE, consume=True)
    def go(self, message: RawRecord) -> RawRecord:
        if message.value.get("name") == "stop":
            raise PipelineStopped("sentinel reached")
        return message


class LifecycleRecorder(Handler):
    def __init__(self) -> None:
        self.seen: list = []

    @step(Stage.MONITOR, consume=True)
    def on_checkpoint(self, message: Checkpoint) -> None:
        self.seen.append("Checkpoint")

    @step(Stage.MONITOR, consume=True)
    def on_shutdown(self, message: Shutdown) -> None:
        self.seen.append("Shutdown")


@pytest.mark.asyncio
async def test_pipeline_stopped_ends_the_run_successfully():
    source = IterableSource([{"name": "ada"}, {"name": "stop"}, {"name": "grace"}])
    pipeline = Pipeline()
    sink = InMemorySink()
    pipeline.add_handler(StopOnSentinel())
    pipeline.add_handler(UppercaseTransform())
    pipeline.add_handler(sink)

    await run(source, pipeline)  # must return, not raise

    assert sink.records == [{"name": "ADA"}]
    assert sink.errors == []


@pytest.mark.asyncio
async def test_early_stop_emits_shutdown_but_not_checkpoint():
    recorder = LifecycleRecorder()
    pipeline = Pipeline()
    pipeline.add_handler(StopOnSentinel())
    pipeline.add_handler(recorder)

    await run(IterableSource([{"name": "stop"}]), pipeline)

    assert recorder.seen == ["Shutdown"]


@pytest.mark.asyncio
async def test_full_run_emits_checkpoint_then_shutdown():
    recorder = LifecycleRecorder()
    pipeline = Pipeline()
    pipeline.add_handler(StopOnSentinel())
    pipeline.add_handler(recorder)

    await run(IterableSource([{"name": "ada"}]), pipeline)

    assert recorder.seen == ["Checkpoint", "Shutdown"]


class SlowSource:
    """Yields one record per item, sleeping between them so run()'s
    per-record tick_interval check gets a chance to fire more than once."""

    def __init__(self, values: list, delay: float) -> None:
        self._values = values
        self._delay = delay

    async def get_records(self):
        for value in self._values:
            await asyncio.sleep(self._delay)
            yield RawRecord(value=value, time=0.0)


class TickRecorder(Handler):
    def __init__(self) -> None:
        self.ticks = 0

    @step(Stage.MONITOR)
    def on_tick(self, message: Tick) -> None:
        self.ticks += 1


@pytest.mark.asyncio
async def test_run_injects_ticks_at_the_given_interval():
    source: Source = SlowSource([{"n": 1}, {"n": 2}, {"n": 3}], delay=0.02)
    pipeline = Pipeline()
    recorder = TickRecorder()
    pipeline.add_handler(recorder)

    await run(source, pipeline, tick_interval=0.01)

    assert recorder.ticks >= 1


@pytest.mark.asyncio
async def test_run_with_no_tick_interval_never_emits_a_tick():
    pipeline = Pipeline()
    recorder = TickRecorder()
    pipeline.add_handler(recorder)

    await run(IterableSource([{"name": "ada"}]), pipeline)

    assert recorder.ticks == 0
