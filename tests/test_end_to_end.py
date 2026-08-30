import pytest

from pipeloom.core import Handler, Pipeline, step
from pipeloom.dataio.sinks import InMemorySink
from pipeloom.dataio.sources import IterableSource
from pipeloom.events import Checkpoint, RawRecord, Record, Shutdown
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
