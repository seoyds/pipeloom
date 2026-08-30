import time
from contextlib import suppress
from typing import Optional

from pipeloom.core import Pipeline
from pipeloom.dataio.sources import Source
from pipeloom.messages import Checkpoint, Shutdown, Tick
from pipeloom.exceptions import PipelineStopped


async def run(source: Source, pipeline: Pipeline, tick_interval: Optional[float] = None) -> None:
    """Drive every record from source through pipeline, then emit final
    Checkpoint and Shutdown messages so MONITOR-stage handlers can react.

    If tick_interval is given, a Tick is processed whenever at least that many
    seconds have elapsed since the last one -- checked once per incoming
    record, not on a background clock, so it cannot fire while the source
    itself is blocked producing the next record.

    A Handler raising PipelineStopped ends the run successfully."""
    # An early stop skips Checkpoint -- a stopped run has no complete unit of work
    # to commit -- but still emits Shutdown so handlers can release resources.
    with suppress(PipelineStopped):
        last_tick = time.monotonic()
        async for record in source.get_records():
            if tick_interval is not None and time.monotonic() - last_tick >= tick_interval:
                await pipeline.process(Tick())
                last_tick = time.monotonic()
            await pipeline.process(record)
        await pipeline.process(Checkpoint())
    with suppress(PipelineStopped):
        await pipeline.process(Shutdown())
