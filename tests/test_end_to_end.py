import pytest

from pipeloom.core import Handler, Pipeline, step
from pipeloom.dataio.sinks import InMemorySink
from pipeloom.dataio.sources import IterableSource
from pipeloom.events import RawRecord, Record
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
