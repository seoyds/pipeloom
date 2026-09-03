"""A minimal PipelineService example you can run or step through in a debugger.

Wraps the same validate/uppercase pipeline as sample_pipeline.py, but drives
it via PipelineService.process_one instead of run() over a Source -- feeding
rows in one at a time and getting back exactly what each one produced.
Demonstrates: an ordinary validation failure surfacing as an ErrorMessage in
the result list (errors-as-data, same as run()), a row-scoped PipelineFailed
that fails only its own call without stopping later rows, an independently
registered sink still seeing every message, and two rows submitted
concurrently still being processed one at a time (non-blocking submission,
serial execution).
"""

import asyncio
import logging

from pipeloom.core import Handler, Pipeline, step
from pipeloom.dataio.sinks import InMemorySink  # noqa: F401 -- registers sink_type= "memory"
from pipeloom.exceptions import PipelineFailed
from pipeloom.messages import RawRecord, Record
from pipeloom.service import PipelineService
from pipeloom.stages import Stage

logger = logging.getLogger(__name__)


class FailOnEmpty(Handler):
    @step(Stage.VALIDATE, consume=True)
    def go(self, message: RawRecord) -> RawRecord:
        if not message.value:
            raise ValueError("empty record")  # becomes an ErrorMessage -- errors as data
        return message


class UppercaseTransform(Handler):
    @step(Stage.TRANSFORM, consume=True)
    def go(self, message: RawRecord) -> Record:
        if message.value.get("name") == "poison":
            raise PipelineFailed("simulated fatal failure")  # bypasses errors-as-data entirely
        return Record({k: str(v).upper() for k, v in message.value.items()})


async def main() -> None:
    pipeline = Pipeline()
    pipeline.add_handler(FailOnEmpty())
    pipeline.add_handler(UppercaseTransform())

    # add_sink still works exactly as under run() -- it sees every message
    # for a submitted row alongside PipelineService's own result collector.
    audit_sink = pipeline.add_sink(sink_type="memory", sink_trigger=Record)

    service = PipelineService(pipeline)

    result = await service.process_one({"name": "ada", "role": "engineer"})
    logger.info("process_one({'name': 'ada', ...}) -> %s", result)

    # An ordinary step failure becomes an ErrorMessage that flows like any
    # other terminal message -- process_one returns it, it doesn't raise.
    result = await service.process_one({})
    logger.info("process_one({}) (invalid row) -> %s", result)

    # A PipelineFailed/PipelineStopped raised by a step, by contrast, fails
    # only that row's call; the service keeps serving rows submitted after
    # it (unlike run(), which would end the whole run).
    try:
        await service.process_one({"name": "poison"})
    except PipelineFailed as exc:
        logger.info("poison row raised PipelineFailed as expected: %s", exc)

    result = await service.process_one({"name": "grace", "role": "admiral"})
    logger.info("process_one({'name': 'grace', ...}) -> %s", result)

    # Submission never blocks on another row's processing -- both calls are
    # queued immediately -- but the pipeline still runs them one at a time.
    results = await asyncio.gather(
        service.process_one({"name": "hopper", "role": "commodore"}),
        service.process_one({"name": "lovelace", "role": "mathematician"}),
    )
    logger.info("concurrent submissions -> %s", results)

    logger.info("audit_sink.received (every Record, independent of process_one) -> %s", audit_sink.received)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-5s %(name)s: %(message)s")
    asyncio.run(main())
