from pipeloom.core import Pipeline
from pipeloom.dataio.sources import Source
from pipeloom.events import Checkpoint, Shutdown


async def run(source: Source, pipeline: Pipeline) -> None:
    """Drive every record from source through pipeline, then emit final
    Checkpoint and Shutdown messages so MONITOR-stage handlers can react."""
    async for record in source.get_records():
        await pipeline.process(record)
    await pipeline.process(Checkpoint())
    await pipeline.process(Shutdown())
