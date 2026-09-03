"""Request/response wrapper around a Pipeline -- see proposal.md/design.md
under openspec/changes/add-pipeline-service for the reasoning behind the
single-worker-loop, row-scoped-fatal-error design."""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from typing import List, Optional

from pipeloom.core import Pipeline, step
from pipeloom.dataio.sinks import Sink
from pipeloom.exceptions import PipelineFailed, PipelineStopped
from pipeloom.messages import ErrorMessage, Message, RawRecord, Record
from pipeloom.stages import Stage

__all__ = ["CorrelatedCollector", "PipelineService"]


class CorrelatedCollector(Sink):
    """A Sink Handler that appends every Record/ErrorMessage it sees, in
    order, into the list it was constructed with. PipelineService registers
    one of these via add_sink and reuses it across every row: since its
    worker only ever has one row in flight at a time, resetting that same
    list before each pipeline.process() call is enough to correlate results
    with the row that produced them -- no per-row isolation mechanism
    needed."""

    def __init__(self, results: List[Message]) -> None:
        self._results = results

    @step(Stage.LOAD)
    def collect_record(self, message: Record) -> None:
        self._results.append(message)

    @step(Stage.LOAD)
    def collect_error(self, message: ErrorMessage) -> None:
        self._results.append(message)


@dataclass
class _Job:
    value: dict
    future: "asyncio.Future[List[Message]]"


class PipelineService:
    """Wraps a pre-built Pipeline so a caller can submit one row at a time
    and get back every Record/ErrorMessage that row produced.

    Submission (process_one) never blocks on another row's processing --
    rows are queued and run one at a time by a single internal worker loop,
    so every existing Handler's single-consumer assumption (safe mutable
    instance state) still holds. A PipelineStopped/PipelineFailed raised
    while processing a row fails only that row's process_one call; the
    worker keeps serving rows submitted afterward -- unlike run(), where the
    same exceptions end the whole run.

    There is no shutdown/close: a persistent service has no natural
    "exhausted" point, so nothing to drain by default."""

    def __init__(self, pipeline: Pipeline, maxsize: int = 0) -> None:
        self._pipeline = pipeline
        self._queue: "asyncio.Queue[_Job]" = asyncio.Queue(maxsize=maxsize)
        self._worker_task: Optional["asyncio.Task[None]"] = None
        self._results: List[Message] = []
        pipeline.add_sink(CorrelatedCollector(self._results))

    def _ensure_worker(self) -> None:
        if self._worker_task is None:
            self._worker_task = asyncio.create_task(self._worker())

    async def process_one(self, value: dict) -> List[Message]:
        """Submit one row's data and await every terminal message (Record/
        ErrorMessage) it produced, in emission order. Raises PipelineStopped
        or PipelineFailed if processing this row hit one -- that failure is
        scoped to this call and does not affect later submissions."""
        self._ensure_worker()
        future: "asyncio.Future[List[Message]]" = asyncio.get_running_loop().create_future()
        await self._queue.put(_Job(value=value, future=future))
        return await future

    async def _worker(self) -> None:
        while True:
            job = await self._queue.get()
            self._results.clear()
            try:
                await self._pipeline.process(RawRecord(value=job.value, time=time.time()))
            except (PipelineStopped, PipelineFailed) as exc:
                job.future.set_exception(exc)
            else:
                job.future.set_result(list(self._results))
            finally:
                self._queue.task_done()
