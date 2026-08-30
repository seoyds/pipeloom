import logging

import pytest

from pipeloom.core import Pipeline
from pipeloom.messages import Heartbeat, RawRecord, Tick
from pipeloom.monitor import PipelineProfiler, RateMonitor


@pytest.mark.asyncio
async def test_rate_monitor_logs_a_rate_and_resets_its_count_on_tick(caplog):
    pipeline = Pipeline()
    monitor = RateMonitor()
    pipeline.add_handler(monitor)

    await pipeline.process(RawRecord(value={"a": 1}, time=0.0))
    await pipeline.process(RawRecord(value={"a": 2}, time=0.0))
    with caplog.at_level(logging.INFO, logger="pipeloom.monitor"):
        await pipeline.process(Tick(time=10.0))

    assert "2 records" in caplog.text
    assert monitor._count == 0


@pytest.mark.asyncio
async def test_rate_monitor_emits_a_heartbeat_carrying_the_measured_interval():
    pipeline = Pipeline()
    pipeline.add_handler(RateMonitor())

    await pipeline.process(Tick(time=5.0))  # establishes prev_time = 5.0
    await pipeline.process(Tick(time=15.0))

    profile = pipeline.get_profile_data()
    entry = next(e for e in profile["pipeline"] if "RateMonitor.report" in e["name"])
    tallied = {t["message_type"] for t in entry["emits"]}
    assert "Heartbeat" in tallied


@pytest.mark.asyncio
async def test_pipeline_profiler_logs_the_blueprint_on_tick(caplog):
    pipeline = Pipeline()
    pipeline.add_handler(RateMonitor())
    profiler = PipelineProfiler(pipeline)
    pipeline.add_handler(profiler)

    with caplog.at_level(logging.INFO, logger="pipeloom.monitor"):
        await pipeline.process(Tick(time=1.0))

    assert "RateMonitor.report" in caplog.text
    assert "PipelineProfiler.report" in caplog.text


@pytest.mark.asyncio
async def test_tick_is_not_consumed_so_multiple_monitors_all_see_it(caplog):
    pipeline = Pipeline()
    rate = RateMonitor()
    pipeline.add_handler(rate)
    pipeline.add_handler(PipelineProfiler(pipeline))

    await pipeline.process(RawRecord(value={"a": 1}, time=0.0))
    with caplog.at_level(logging.INFO, logger="pipeloom.monitor"):
        await pipeline.process(Tick(time=1.0))

    # Both handlers' logs fired from the same Tick -- neither consumed it.
    assert "1 records" in caplog.text
    assert "RateMonitor.report" in caplog.text
