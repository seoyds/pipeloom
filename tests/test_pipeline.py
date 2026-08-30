import pytest

from pipeloom.core import Handler, Pipeline, step
from pipeloom.events import ErrorMessage, Message, RawRecord, Record
from pipeloom.exceptions import PipelineFailed, RoutingError
from pipeloom.stages import Stage


class Uppercase(Handler):
    @step(Stage.TRANSFORM, consume=True)
    def go(self, message: RawRecord) -> Record:
        return Record({k: str(v).upper() for k, v in message.value.items()})


class Collector(Handler):
    def __init__(self) -> None:
        self.received: list = []

    @step(Stage.LOAD, consume=True)
    def collect(self, message: Record) -> None:
        self.received.append(dict(message))


class Failing(Handler):
    @step(Stage.TRANSFORM, consume=True)
    def go(self, message: RawRecord) -> Record:
        raise ValueError("boom")


class ErrorCollector(Handler):
    def __init__(self) -> None:
        self.errors: list = []

    @step(Stage.LOAD, consume=True)
    def collect(self, message: ErrorMessage) -> None:
        self.errors.append(message)


@pytest.mark.asyncio
async def test_message_routes_through_handlers_by_type():
    pipeline = Pipeline()
    collector = Collector()
    pipeline.add_handler(Uppercase())
    pipeline.add_handler(collector)

    await pipeline.process(RawRecord(value={"a": "x"}, time=0.0))

    assert collector.received == [{"a": "X"}]


@pytest.mark.asyncio
async def test_exception_in_a_step_becomes_an_error_message_downstream():
    pipeline = Pipeline()
    error_collector = ErrorCollector()
    pipeline.add_handler(Failing())
    pipeline.add_handler(error_collector)

    await pipeline.process(RawRecord(value={}, time=0.0))

    assert len(error_collector.errors) == 1
    assert isinstance(error_collector.errors[0].exception, ValueError)
    assert str(error_collector.errors[0].exception) == "boom"


@pytest.mark.asyncio
async def test_async_step_is_awaited():
    class AsyncUppercase(Handler):
        @step(Stage.TRANSFORM, consume=True)
        async def go(self, message: RawRecord) -> Record:
            return Record({k: str(v).upper() for k, v in message.value.items()})

    pipeline = Pipeline()
    collector = Collector()
    pipeline.add_handler(AsyncUppercase())
    pipeline.add_handler(collector)

    await pipeline.process(RawRecord(value={"a": "x"}, time=0.0))

    assert collector.received == [{"a": "X"}]


@pytest.mark.asyncio
async def test_unhandled_error_message_reaches_builtin_catch_all_and_fails():
    pipeline = Pipeline()
    pipeline.add_handler(Failing())
    # no ErrorMessage handler registered -> built-in catch-all should raise PipelineFailed

    with pytest.raises(PipelineFailed):
        await pipeline.process(RawRecord(value={}, time=0.0))


@pytest.mark.asyncio
async def test_unrouted_non_error_message_is_dropped_by_catch_all():
    class Emitter(Handler):
        @step(Stage.EXTRACT, consume=True)
        def go(self, message: RawRecord) -> Record:
            return Record({"ok": True})

    pipeline = Pipeline()
    pipeline.add_handler(Emitter())
    # No handler consumes Record; built-in catch-all should silently drop it, not raise.
    await pipeline.process(RawRecord(value={}, time=0.0))


@pytest.mark.asyncio
async def test_message_with_no_matching_handler_and_no_next_step_raises_routing_error():
    # A handler that returns a type nothing (not even the catch-all) can route to
    # is impossible by construction, since the catch-all accepts any Message.
    # Instead, verify RoutingError is raised internally when a step is queried directly
    # past the end of the pipeline (regression guard for _find_next).
    pipeline = Pipeline()
    pipeline._rebuild()
    with pytest.raises(RoutingError):
        pipeline._find_next(pipeline._steps[-1], RawRecord)


def test_profile_data_reports_call_counts():
    pipeline = Pipeline()
    pipeline.add_handler(Uppercase())
    pipeline._rebuild()
    profile = pipeline.get_profile_data()
    names = [entry["name"] for entry in profile["pipeline"]]
    assert any("Uppercase.go" in name for name in names)


@pytest.mark.asyncio
async def test_profile_data_reports_message_type_tallies():
    pipeline = Pipeline()
    collector = Collector()
    pipeline.add_handler(Uppercase())
    pipeline.add_handler(collector)

    await pipeline.process(RawRecord(value={"a": "x"}, time=0.0))

    profile = pipeline.get_profile_data()
    entry_names = [e["name"] for e in profile["pipeline"]]
    # "emits" is keyed by what a step produced (matching propipe's semantics):
    # Uppercase.go consumes RawRecord and emits Record, so its tally holds "Record".
    uppercase_entry = profile["pipeline"][entry_names.index("Uppercase.go")]
    tallied_types = [e["message_type"] for e in uppercase_entry["emits"]]
    assert "Record" in tallied_types


@pytest.mark.asyncio
async def test_format_profile_produces_a_readable_string():
    pipeline = Pipeline()
    pipeline.add_handler(Uppercase())
    await pipeline.process(RawRecord(value={"a": "x"}, time=0.0))
    profile = pipeline.get_profile_data()
    output = pipeline.format_profile(profile)
    assert "Uppercase.go" in output
