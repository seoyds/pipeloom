import asyncio

import pytest

from pipeloom.core import Handler, Pipeline, step
from pipeloom.exceptions import PipelineFailed, PipelineStopped
from pipeloom.messages import ErrorMessage, RawRecord, Record
from pipeloom.service import CorrelatedCollector, PipelineService
from pipeloom.stages import Stage


@pytest.mark.asyncio
async def test_correlated_collector_appends_records_and_errors_in_order():
    results: list = []
    pipeline = Pipeline()
    pipeline.add_sink(CorrelatedCollector(results))

    err = ErrorMessage(message=RawRecord(value={}, time=0.0), exception=ValueError("x"))
    await pipeline.process(Record({"a": 1}))
    await pipeline.process(err)

    assert results == [Record({"a": 1}), err]


@pytest.mark.asyncio
async def test_correlated_collector_coexists_with_another_registered_sink():
    results: list = []
    pipeline = Pipeline()
    pipeline.add_sink(CorrelatedCollector(results))
    memory_sink = pipeline.add_sink(sink_type="memory", sink_trigger=Record)

    await pipeline.process(Record({"a": 1}))

    assert results == [Record({"a": 1})]
    assert memory_sink.received == [{"a": 1}]


class Uppercase(Handler):
    @step(Stage.TRANSFORM, consume=True)
    def go(self, message: RawRecord) -> Record:
        return Record({k: str(v).upper() for k, v in message.value.items()})


@pytest.mark.asyncio
async def test_process_one_returns_the_single_record_it_produced():
    pipeline = Pipeline()
    pipeline.add_handler(Uppercase())
    service = PipelineService(pipeline)

    result = await service.process_one({"a": "x"})

    assert result == [Record({"a": "X"})]


@pytest.mark.asyncio
async def test_process_one_returns_empty_list_when_nothing_terminal_is_produced():
    class Dropper(Handler):
        @step(Stage.VALIDATE, consume=True)
        def go(self, message: RawRecord) -> None:
            return None

    pipeline = Pipeline()
    pipeline.add_handler(Dropper())
    service = PipelineService(pipeline)

    result = await service.process_one({"a": "x"})

    assert result == []


@pytest.mark.asyncio
async def test_process_one_returns_multiple_terminal_messages_in_emission_order():
    class FanOut(Handler):
        @step(Stage.TRANSFORM, consume=True)
        def go(self, message: RawRecord) -> "list[Record]":
            return [Record({"n": 1}), Record({"n": 2}), Record({"n": 3})]

    pipeline = Pipeline()
    pipeline.add_handler(FanOut())
    service = PipelineService(pipeline)

    result = await service.process_one({})

    assert result == [Record({"n": 1}), Record({"n": 2}), Record({"n": 3})]


class OrderRecorder(Handler):
    def __init__(self) -> None:
        self.events: list = []

    @step(Stage.TRANSFORM, consume=True)
    async def go(self, message: RawRecord) -> Record:
        self.events.append(("start", message.value["id"]))
        await asyncio.sleep(0.05)
        self.events.append(("end", message.value["id"]))
        return Record({"id": message.value["id"]})


@pytest.mark.asyncio
async def test_two_rows_submitted_concurrently_are_processed_serially():
    pipeline = Pipeline()
    recorder = OrderRecorder()
    pipeline.add_handler(recorder)
    service = PipelineService(pipeline)

    results = await asyncio.gather(
        service.process_one({"id": "a"}),
        service.process_one({"id": "b"}),
    )

    assert recorder.events == [
        ("start", "a"),
        ("end", "a"),
        ("start", "b"),
        ("end", "b"),
    ]
    assert results == [[Record({"id": "a"})], [Record({"id": "b"})]]


class Gate(Handler):
    def __init__(self) -> None:
        self.entered = asyncio.Event()
        self.release = asyncio.Event()

    @step(Stage.TRANSFORM, consume=True)
    async def go(self, message: RawRecord) -> Record:
        self.entered.set()
        await self.release.wait()
        return Record({"ok": True})


@pytest.mark.asyncio
async def test_process_one_does_not_block_submission_on_a_prior_in_flight_row():
    pipeline = Pipeline()
    gate = Gate()
    pipeline.add_handler(gate)
    service = PipelineService(pipeline)

    task_a = asyncio.create_task(service.process_one({"id": "a"}))
    await gate.entered.wait()  # row a is now stuck mid-step, on the worker

    task_b = asyncio.create_task(service.process_one({"id": "b"}))
    await asyncio.sleep(0)  # let task_b's put() land before row a is released
    assert service._queue.qsize() == 1  # b queued behind a without waiting for it

    gate.release.set()
    result_a = await task_a
    result_b = await task_b

    assert result_a == [Record({"ok": True})]
    assert result_b == [Record({"ok": True})]


class FailForId(Handler):
    def __init__(self, exc_type) -> None:
        self._exc_type = exc_type

    @step(Stage.TRANSFORM, consume=True)
    def go(self, message: RawRecord) -> Record:
        if message.value.get("id") == "bad":
            raise self._exc_type("row-scoped failure")
        return Record({"id": message.value["id"]})


@pytest.mark.asyncio
async def test_pipeline_failed_on_one_row_does_not_stop_later_rows():
    pipeline = Pipeline()
    pipeline.add_handler(FailForId(PipelineFailed))
    service = PipelineService(pipeline)

    with pytest.raises(PipelineFailed):
        await service.process_one({"id": "bad"})

    result = await service.process_one({"id": "good"})
    assert result == [Record({"id": "good"})]


@pytest.mark.asyncio
async def test_pipeline_stopped_on_one_row_does_not_stop_later_rows():
    pipeline = Pipeline()
    pipeline.add_handler(FailForId(PipelineStopped))
    service = PipelineService(pipeline)

    with pytest.raises(PipelineStopped):
        await service.process_one({"id": "bad"})

    result = await service.process_one({"id": "good"})
    assert result == [Record({"id": "good"})]
