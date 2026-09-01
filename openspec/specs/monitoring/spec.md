# monitoring Specification

## Purpose

`Tick`-driven `Handler`s (registered like any other [[pipeline-core]]
handler) that observe pipeline traffic and periodically report on it.
`Tick` is only produced by `run()`'s `tick_interval` parameter.

## Requirements

### Requirement: RateMonitor
`RateMonitor` SHALL tally `RawRecord` throughput and, on each `Tick`, log a
records/sec rate and emit a `Heartbeat`.

#### Scenario: RawRecord throughput is tallied at EXTRACT
- **WHEN** a `RawRecord` passes through the pipeline
- **THEN** `RateMonitor`'s internal count increments (its tally step is registered at `Stage.EXTRACT`)

#### Scenario: A Tick logs the rate and resets the count
- **WHEN** a `Tick` reaches `RateMonitor`'s `Stage.MONITOR` step
- **THEN** it logs the number of records processed and the records/sec rate since the previous `Tick` (or since start, for the first `Tick`), then resets the count to zero

#### Scenario: A Tick emits a Heartbeat
- **WHEN** `RateMonitor` handles a `Tick`
- **THEN** it emits a `Heartbeat(time=<tick time>, prev_time=<previous tick time>)`

#### Scenario: Tick is left un-consumed
- **WHEN** `RateMonitor` handles a `Tick`
- **THEN** the `Tick` is not consumed, so other `MONITOR`-stage handlers can also observe the same `Tick`

### Requirement: PipelineProfiler
`PipelineProfiler(pipeline)` SHALL log its owning `Pipeline`'s blueprint
profile on every `Tick`.

#### Scenario: A Tick logs the pipeline blueprint
- **WHEN** a `Tick` reaches `PipelineProfiler`'s `Stage.MONITOR` step
- **THEN** it logs `pipeline.format_profile(pipeline.get_profile_data(), blueprint=True)` for the `Pipeline` it was constructed with
