"""A minimal pipeloom pipeline you can run or step through in a debugger.

Reads a few in-memory records, validates them, uppercases their fields, and
sinks the results (or errors) in memory -- then prints what came out.
"""

import asyncio
import logging

from pipeloom.core import Handler, Pipeline, step
from pipeloom.dataio.sinks import InMemorySink
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
    sink = InMemorySink()
    pipeline.add_handler(FailOnEmpty())
    pipeline.add_handler(UppercaseTransform())
    pipeline.add_handler(sink)

    await run(source, pipeline)

    logger.info("Records: %s", sink.records)
    logger.info("Errors: %s", [type(e.exception).__name__ for e in sink.errors])
    logger.info(pipeline.format_profile(pipeline.get_profile_data(), blueprint=True))


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-5s %(name)s: %(message)s")
    asyncio.run(main())
