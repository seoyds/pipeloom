from contextlib import suppress

from pipeloom.core import Pipeline
from pipeloom.dataio.sources import Source
from pipeloom.events import Checkpoint, Shutdown
from pipeloom.exceptions import PipelineStopped


async def run(source: Source, pipeline: Pipeline) -> None:
    """Drive every record from source through pipeline, then emit final
    Checkpoint and Shutdown messages so MONITOR-stage handlers can react.

    A Handler raising PipelineStopped ends the run successfully."""
    # An early stop skips Checkpoint -- a stopped run has no complete unit of work
    # to commit -- but still emits Shutdown so handlers can release resources.
    with suppress(PipelineStopped):
        async for record in source.get_records():
            await pipeline.process(record)
        await pipeline.process(Checkpoint())
    with suppress(PipelineStopped):
        await pipeline.process(Shutdown())
