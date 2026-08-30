"""A minimal pipeloom pipeline you can run or step through in a debugger.

Reads a few in-memory records, validates them, uppercases their fields, and
fans the results out to two sinks at once via add_sink -- then prints what
came out. Errors get their own separate sink (sink_trigger=ErrorMessage).
"""

import asyncio
import logging
import tempfile
from pathlib import Path

from pipeloom.core import Handler, Pipeline, step
from pipeloom.dataio.sinks import InMemorySink, JSONLinesSink  # noqa: F401 -- registers sink_type= "memory"/"json"
from pipeloom.dataio.sources import IterableSource
from pipeloom.messages import ErrorMessage, RawRecord, Record
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
    pipeline.add_handler(FailOnEmpty())
    pipeline.add_handler(UppercaseTransform())

    # add_sink (not add_handler) broadcasts every Record to both of these --
    # sink_trigger is required and never defaulted, so it's explicit below.
    memory_sink = pipeline.add_sink(sink_type="memory", sink_trigger=Record)
    json_path = Path(tempfile.gettempdir()) / "pipeloom_sample_output.jsonl"
    json_sink = pipeline.add_sink(sink_type="json", sink_params={"path": json_path}, sink_trigger=Record)
    # A sink only ever fires on one trigger type -- persisting errors too
    # means a separate add_sink call with sink_trigger=ErrorMessage.
    error_sink = pipeline.add_sink(sink_type="memory", sink_trigger=ErrorMessage)

    await run(source, pipeline)
    json_sink.close()

    logger.info("Records: %s", memory_sink.received)
    logger.info("Errors: %s", [type(e.exception).__name__ for e in error_sink.received])
    logger.info("Also written to: %s", json_path)
    logger.info(pipeline.format_profile(pipeline.get_profile_data(), blueprint=True))


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-5s %(name)s: %(message)s")
    asyncio.run(main())
