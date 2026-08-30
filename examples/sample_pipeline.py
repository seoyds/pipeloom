"""A minimal pipeloom pipeline you can run or step through in a debugger.

Reads a few in-memory records, validates them, uppercases their fields, and
fans the results (or errors) out to two sinks at once via add_sink -- then
prints what came out.
"""

import asyncio
import logging
import tempfile
from pathlib import Path

from pipeloom.core import Handler, Pipeline, step
from pipeloom.dataio.sinks import InMemorySink, JSONLinesSink
from pipeloom.dataio.sources import IterableSource
from pipeloom.messages import RawRecord, Record
from pipeloom.pipeline import run
from pipeloom.stages import Stage

logger = logging.getLogger(__name__)


class FailOnEmpty(Handler):
    @step(Stage.VALIDATE, consume=True)
    def go(self, message: RawRecord) -> RawRecord:
        if not message.value:
            raise ValueError("empty record")
        return message


class UppercaseTransform(Handler):
    @step(Stage.TRANSFORM, consume=True)
    def go(self, message: RawRecord) -> Record:
        return Record({k: str(v).upper() for k, v in message.value.items()})


async def main() -> None:
    source = IterableSource(
        [
            {"name": "ada", "role": "engineer"},
            {},  # deliberately invalid, routes to sink.errors
            {"name": "grace", "role": "admiral"},
        ]
    )
    pipeline = Pipeline()
    memory_sink = InMemorySink()
    json_path = Path(tempfile.gettempdir()) / "pipeloom_sample_output.jsonl"
    json_sink = JSONLinesSink(json_path)
    pipeline.add_handler(FailOnEmpty())
    pipeline.add_handler(UppercaseTransform())
    # add_sink (not add_handler) broadcasts each Record/ErrorMessage to every
    # sink registered this way -- both of these see every record.
    pipeline.add_sink(memory_sink)
    pipeline.add_sink(json_sink)

    await run(source, pipeline)
    json_sink.close()

    logger.info("Records: %s", memory_sink.records)
    logger.info("Errors: %s", [type(e.exception).__name__ for e in memory_sink.errors])
    logger.info("Also written to: %s", json_path)
    logger.info(pipeline.format_profile(pipeline.get_profile_data(), blueprint=True))


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-5s %(name)s: %(message)s")
    asyncio.run(main())
