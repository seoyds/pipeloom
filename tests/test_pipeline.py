import collections.abc
import signal
from contextlib import contextmanager
from typing import Iterable

import pytest

from pipeloom.core import Handler, Pipeline, step
from pipeloom.messages import ErrorMessage, Message, RawRecord, Record
from pipeloom.exceptions import PipelineError, PipelineFailed, RoutingError
from pipeloom.stages import Stage


class _TookTooLong(BaseException):
    # BaseException, not Exception: process()'s own `except Exception` would
    # otherwise swallow the alarm and keep spinning.
    pass


@contextmanager
def must_not_hang(seconds: float = 5.0):
    # process() spins in pure Python with no await point, so an infinite loop
    # cannot be interrupted by asyncio.wait_for -- SIGALRM is what gets through.
    def _alarm(signum, frame):
        raise _TookTooLong

    previous = signal.signal(signal.SIGALRM, _alarm)
    signal.setitimer(signal.ITIMER_REAL, seconds)
    try:
        yield
    except _TookTooLong:
        pytest.fail(f"process() did not return within {seconds}s")
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous)


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


class _ReasonedError(Exception):
    """Stands in for any exception exposing a .reason attribute (as
    pipeloom.agents' AgentCallFailed does), without importing the agents
    package -- core must stay agent-agnostic."""

    def __init__(self, message: str, reason: str) -> None:
        super().__init__(message)
        self.reason = reason


class FailingWithReason(Handler):
    @step(Stage.TRANSFORM, consume=True)
    def go(self, message: RawRecord) -> Record:
        raise _ReasonedError("boom", reason="input_required")


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
async def test_error_message_carries_reason_from_an_exception_that_exposes_one():
    pipeline = Pipeline()
    error_collector = ErrorCollector()
    pipeline.add_handler(FailingWithReason())
    pipeline.add_handler(error_collector)

    await pipeline.process(RawRecord(value={}, time=0.0))

    assert len(error_collector.errors) == 1
    assert error_collector.errors[0].reason == "input_required"


@pytest.mark.asyncio
async def test_error_message_reason_is_none_for_an_exception_without_one():
    pipeline = Pipeline()
    error_collector = ErrorCollector()
    pipeline.add_handler(Failing())  # raises a plain ValueError
    pipeline.add_handler(error_collector)

    await pipeline.process(RawRecord(value={}, time=0.0))

    assert len(error_collector.errors) == 1
    assert error_collector.errors[0].reason is None


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


@pytest.mark.asyncio
async def test_format_profile_emits_stage_banners_by_default():
    pipeline = Pipeline()
    pipeline.add_handler(Uppercase())
    await pipeline.process(RawRecord(value={"a": "x"}, time=0.0))
    output = pipeline.format_profile(pipeline.get_profile_data())
    assert "[ TRANSFORM = 40 ]" in output


@pytest.mark.asyncio
async def test_step_returning_a_bare_dict_errors_instead_of_hanging():
    class ForgotToWrap(Handler):
        @step(Stage.TRANSFORM, consume=True)
        def go(self, message: RawRecord) -> Record:
            return {"a": 1}  # forgot Record(...)

    pipeline = Pipeline()
    errors = ErrorCollector()
    pipeline.add_handler(ForgotToWrap())
    pipeline.add_handler(errors)

    with must_not_hang():
        await pipeline.process(RawRecord(value={}, time=0.0))

    assert len(errors.errors) == 1
    exc = errors.errors[0].exception
    assert isinstance(exc, PipelineError)
    assert "ForgotToWrap.go" in str(exc)


@pytest.mark.asyncio
async def test_step_returning_a_list_with_a_non_message_errors_instead_of_hanging():
    class MixedFanOut(Handler):
        @step(Stage.TRANSFORM, consume=True)
        def go(self, message: RawRecord) -> Iterable[Record]:
            return [Record({"ok": True}), "not a message"]

    pipeline = Pipeline()
    errors = ErrorCollector()
    pipeline.add_handler(MixedFanOut())
    pipeline.add_handler(errors)

    with must_not_hang():
        await pipeline.process(RawRecord(value={}, time=0.0))

    assert len(errors.errors) == 1
    exc = errors.errors[0].exception
    assert isinstance(exc, PipelineError)
    assert "MixedFanOut.go" in str(exc)
    assert "not a message" in str(exc)


@pytest.mark.asyncio
async def test_default_consume_false_requeues_the_original_message():
    class Observe(Handler):
        def __init__(self) -> None:
            self.seen: list = []

        @step(Stage.VALIDATE)  # default consume=False
        def observe(self, message: RawRecord) -> None:
            self.seen.append(dict(message.value))

    pipeline = Pipeline()
    observer = Observe()
    collector = Collector()
    pipeline.add_handler(observer)
    pipeline.add_handler(Uppercase())
    pipeline.add_handler(collector)

    await pipeline.process(RawRecord(value={"a": "x"}, time=0.0))

    assert observer.seen == [{"a": "x"}]
    # The RawRecord was re-queued after the non-consuming step and kept routing.
    assert collector.received == [{"a": "X"}]


@pytest.mark.asyncio
async def test_step_returning_an_iterable_fans_out_in_order():
    class FanOut(Handler):
        @step(Stage.TRANSFORM, consume=True)
        def go(self, message: RawRecord) -> Iterable[Record]:
            return [Record({"n": 1}), Record({"n": 2}), Record({"n": 3})]

    pipeline = Pipeline()
    collector = Collector()
    pipeline.add_handler(FanOut())
    pipeline.add_handler(collector)

    await pipeline.process(RawRecord(value={}, time=0.0))

    assert collector.received == [{"n": 1}, {"n": 2}, {"n": 3}]


@pytest.mark.asyncio
async def test_generator_step_fans_out_in_order():
    class GeneratorFanOut(Handler):
        @step(Stage.TRANSFORM, consume=True)
        def go(self, message: RawRecord) -> Iterable[Record]:
            for n in (1, 2):
                yield Record({"n": n})

    pipeline = Pipeline()
    collector = Collector()
    pipeline.add_handler(GeneratorFanOut())
    pipeline.add_handler(collector)

    await pipeline.process(RawRecord(value={}, time=0.0))

    assert collector.received == [{"n": 1}, {"n": 2}]


@pytest.mark.asyncio
async def test_format_profile_blueprint_shows_types_and_docstring():
    class Documented(Handler):
        @step(Stage.TRANSFORM, consume=True)
        def go(self, message: RawRecord) -> Record:
            """Uppercases every field."""
            return Record({k: str(v).upper() for k, v in message.value.items()})

    pipeline = Pipeline()
    pipeline.add_handler(Documented())
    await pipeline.process(RawRecord(value={"a": "x"}, time=0.0))

    output = pipeline.format_profile(pipeline.get_profile_data(), blueprint=True)

    assert "Documented.go(RawRecord)" in output
    assert "consumed" in output
    assert "Record" in output
    assert "ms/call" in output
    assert "Uppercases every field." in output


def test_step_accepts_collections_abc_iterable_return_annotation():
    class AbcIterableFanOut(Handler):
        @step(Stage.TRANSFORM, consume=True)
        def go(self, message: RawRecord) -> collections.abc.Iterable[Record]:
            return [Record({"ok": True})]

    # Must not raise AssertionError at decoration time.
    AbcIterableFanOut()


class RecordingSink(Handler):
    def __init__(self) -> None:
        self.received: list = []

    @step(Stage.LOAD, consume=True)
    def sink_record(self, message: Record) -> None:
        self.received.append(dict(message))


@pytest.mark.asyncio
async def test_add_sink_broadcasts_to_every_registered_sink():
    pipeline = Pipeline()
    sink_a = RecordingSink()
    sink_b = RecordingSink()
    pipeline.add_handler(Uppercase())
    pipeline.add_sink(sink_a)
    pipeline.add_sink(sink_b)

    await pipeline.process(RawRecord(value={"a": "x"}, time=0.0))

    assert sink_a.received == [{"a": "X"}]
    assert sink_b.received == [{"a": "X"}]


@pytest.mark.asyncio
async def test_add_sink_message_never_reaches_add_handler_routed_steps():
    pipeline = Pipeline()
    collector = Collector()  # add_handler-routed, would also consume Record
    sink = RecordingSink()
    pipeline.add_handler(Uppercase())
    pipeline.add_handler(collector)
    pipeline.add_sink(sink)

    await pipeline.process(RawRecord(value={"a": "x"}, time=0.0))

    assert sink.received == [{"a": "X"}]
    assert collector.received == []  # add_sink took it first


@pytest.mark.asyncio
async def test_add_sink_isolates_one_failing_sink_from_the_others():
    class BrokenSink(Handler):
        @step(Stage.LOAD, consume=True)
        def sink_record(self, message: Record) -> None:
            raise ValueError("disk full")

    pipeline = Pipeline()
    broken = BrokenSink()
    healthy = RecordingSink()
    errors = ErrorCollector()
    pipeline.add_sink(broken)
    pipeline.add_sink(healthy)
    pipeline.add_handler(errors)  # add_handler: ErrorMessage isn't sink-registered

    await pipeline.process(Record({"a": 1}))

    assert healthy.received == [{"a": 1}]
    assert len(errors.errors) == 1
    assert isinstance(errors.errors[0].exception, ValueError)


@pytest.mark.asyncio
async def test_add_sink_registering_the_same_sink_twice_raises():
    pipeline = Pipeline()
    sink = RecordingSink()
    pipeline.add_sink(sink)
    with pytest.raises(ValueError):
        pipeline.add_sink(sink)


@pytest.mark.asyncio
async def test_remove_sink_stops_it_from_receiving_further_messages():
    pipeline = Pipeline()
    sink = RecordingSink()
    pipeline.add_sink(sink)
    await pipeline.process(Record({"a": 1}))
    pipeline.remove_sink(sink)

    # No longer sink-consumed; falls through to the catch-all's silent drop.
    await pipeline.process(Record({"a": 2}))

    assert sink.received == [{"a": 1}]


@pytest.mark.asyncio
async def test_handlers_run_in_stage_order_regardless_of_registration_order():
    pipeline = Pipeline()
    collector = Collector()  # Stage.LOAD
    pipeline.add_handler(collector)
    pipeline.add_handler(Uppercase())  # Stage.TRANSFORM, registered second

    await pipeline.process(RawRecord(value={"a": "x"}, time=0.0))

    assert [s.name for s in pipeline._steps[:2]] == ["Uppercase.go", "Collector.collect"]
    assert collector.received == [{"a": "X"}]
