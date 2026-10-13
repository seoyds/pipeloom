"""Celery as the outer distribution layer, pipeloom as the per-chunk engine.

Pattern: Celery decides *where* work runs (which worker, which machine, with
what retry policy); pipeloom decides *what happens* to each row once a task
picks it up (validate/transform/load, error-as-data, profiling). One Celery
task = one chunk of rows, processed through a fresh Pipeline via run().

A task should not round-trip full row data back through Celery's result
backend -- that backend is meant for small payloads/status, not bulk row
data. So each task writes its real output to a Sink (a JSON sink here --
swap in the db/kafka/parquet sinks for a real deployment) and returns only
counts. That also matches how a Handler's errors-as-data model already
works: a row failing validation doesn't fail the whole chunk's task, it
just gets counted and persisted like everything else.

This file uses CELERY_TASK_ALWAYS_EAGER so it runs standalone with no
broker/worker needed, exactly like the other sample_pipeline_*.py files --
see main() for what changes in a real deployment.
"""

import asyncio
import logging
import os
import tempfile
from pathlib import Path
from typing import Iterator, List, Sequence, TypeVar

from celery import Celery

from pipeloom.core import Handler, Pipeline, step
from pipeloom.dataio.sinks import JSONLinesSink  # noqa: F401 -- registers sink_type="json"
from pipeloom.dataio.sources import IterableSource
from pipeloom.messages import ErrorMessage, RawRecord, Record
from pipeloom.pipeline import run
from pipeloom.stages import Stage

logger = logging.getLogger(__name__)

T = TypeVar("T")


class FailOnEmpty(Handler):
    @step(Stage.VALIDATE, consume=True)
    def go(self, message: RawRecord) -> RawRecord:
        if not message.value:
            raise ValueError("empty record")  # becomes an ErrorMessage -- errors as data
        return message


class UppercaseTransform(Handler):
    @step(Stage.TRANSFORM, consume=True)
    def go(self, message: RawRecord) -> Record:
        return Record({k: str(v).upper() for k, v in message.value.items()})


def build_pipeline(output_path: Path) -> Pipeline:
    """A fresh Pipeline per task call -- these Handlers are stateless, so
    there's no reason to share one across chunks. If a chunk needed
    cross-row state (a Batcher, say), you'd build the Pipeline once per
    Celery worker process instead, via the worker_process_init signal."""
    pipeline = Pipeline()
    pipeline.add_handler(FailOnEmpty())
    pipeline.add_handler(UppercaseTransform())
    json_sink = pipeline.add_sink(sink_type="json", sink_params={"path": output_path}, sink_trigger=Record)
    error_sink = pipeline.add_sink(sink_type="memory", sink_trigger=ErrorMessage)
    return pipeline, json_sink, error_sink


async def _process_chunk(rows: List[dict], output_path: Path) -> dict:
    pipeline, json_sink, error_sink = build_pipeline(output_path)
    source = IterableSource(rows)
    await run(source, pipeline)
    json_sink.close()
    return {
        "written": len(rows) - len(error_sink.received),
        "errors": len(error_sink.received),
    }


# broker/backend read from env so the same module works against a real
# Redis/RabbitMQ in production without code changes -- the memory:// /
# cache+memory:// defaults below only work in-process under
# task_always_eager (see main()) and are demo-only, e.g.:
#   CELERY_BROKER_URL=redis://localhost:6379/0
#   CELERY_RESULT_BACKEND=redis://localhost:6379/1
app = Celery(
    "pipeloom_worker",
    broker=os.environ.get("CELERY_BROKER_URL", "memory://"),
    backend=os.environ.get("CELERY_RESULT_BACKEND", "cache+memory://"),
)


@app.task(name="pipeloom.process_chunk")
def process_chunk(rows: List[dict], output_path: str) -> dict:
    """The Celery task body: sync, since Celery doesn't await tasks -- the
    only async code is pipeloom's, bridged with one asyncio.run() per task
    invocation."""
    return asyncio.run(_process_chunk(rows, Path(output_path)))


def chunked(items: Sequence[T], size: int) -> Iterator[List[T]]:
    for i in range(0, len(items), size):
        yield list(items[i : i + size])


def main() -> None:
    # Demo-only: runs tasks inline, in this process, with no broker or
    # `celery -A examples.sample_pipeline_celery worker` needed. In a real
    # deployment, drop this line, start actual workers, and call
    # process_chunk.delay(...) instead of process_chunk.apply(...) below --
    # .delay() enqueues onto the broker and returns immediately; .apply()
    # (used here) runs synchronously in-process, which is what makes eager
    # mode usable without a worker at all.
    app.conf.update(task_always_eager=True, task_eager_propagates=True)

    rows = [
        {"name": "ada", "role": "engineer"},
        {"name": "grace", "role": "admiral"},
        {},  # deliberately invalid
        {"name": "alan", "role": "mathematician"},
        {"name": "margaret", "role": "programmer"},
        {"name": "katherine", "role": "physicist"},
        {},  # deliberately invalid
    ]
    output_dir = Path(tempfile.mkdtemp(prefix="pipeloom_celery_"))

    # Shard rows into fixed-size chunks, dispatch one Celery task per chunk,
    # each writing to its own output file (a real deployment would point
    # every chunk at the same durable sink -- a DB table or Kafka topic --
    # rather than per-chunk files).
    async_results = []
    for i, chunk in enumerate(chunked(rows, size=3)):
        output_path = output_dir / f"chunk_{i}.jsonl"
        async_results.append(process_chunk.apply(args=[chunk, str(output_path)]))

    # In production this .get() loop would run in a separate process from
    # the one that called .delay() -- the producer doesn't have to block on
    # it at all, or could collect results via a Celery `group`/`chord`.
    totals = {"written": 0, "errors": 0}
    for i, async_result in enumerate(async_results):
        result = async_result.get(timeout=10)
        logger.info("chunk %d -> %s", i, result)
        totals["written"] += result["written"]
        totals["errors"] += result["errors"]

    logger.info("totals across all chunks -> %s", totals)
    logger.info("chunk outputs written under %s", output_dir)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-5s %(name)s: %(message)s")
    main()
