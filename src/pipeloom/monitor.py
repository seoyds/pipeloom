"""MONITOR-stage Handlers that observe pipeline traffic and periodically
report on it. Both Handlers here react to Tick -- see pipeloom.pipeline.run's
tick_interval parameter, the only producer of Tick in this package -- and
leave it un-consumed so multiple monitors can each see the same Tick.
"""

import logging
from typing import Optional

from pipeloom.core import Handler, Pipeline, step
from pipeloom.messages import Heartbeat, RawRecord, Tick
from pipeloom.stages import Stage

logger = logging.getLogger(__name__)


class RateMonitor(Handler):
    """Tracks RawRecord throughput and logs a records/sec rate on every Tick.

    Also emits a Heartbeat carrying the interval it just measured, for any
    MONITOR-stage Handler that wants a liveness/cadence signal without
    re-deriving it from Tick timestamps itself.
    """

    def __init__(self) -> None:
        self._count = 0
        self._prev_time: Optional[float] = None

    @step(Stage.EXTRACT)
    def tally(self, message: RawRecord) -> None:
        self._count += 1

    @step(Stage.MONITOR)
    def report(self, message: Tick) -> Heartbeat:
        prev_time = self._prev_time if self._prev_time is not None else message.time
        elapsed = message.time - prev_time
        rate = (self._count / elapsed) if elapsed > 0 else 0.0
        logger.info("processed %d records (%.1f records/sec)", self._count, rate)
        heartbeat = Heartbeat(time=message.time, prev_time=prev_time)
        self._count = 0
        self._prev_time = message.time
        return heartbeat


class PipelineProfiler(Handler):
    """Logs the owning Pipeline's blueprint/profile on every Tick."""

    def __init__(self, pipeline: Pipeline) -> None:
        self._pipeline = pipeline

    @step(Stage.MONITOR)
    def report(self, message: Tick) -> None:
        profile = self._pipeline.get_profile_data()
        logger.info(self._pipeline.format_profile(profile, blueprint=True))
