# pipeloom

An async ETL pipeline where steps may be local code or AI agents reached over A2A.

Handlers declare typed `@step`s; `Pipeline` flattens them into one stage-ordered
chain and routes each `Message` by its type. Failures become data (an
`ErrorMessage`) instead of crashing the run.

## Install

```bash
uv sync                      # core (zero runtime dependencies)
uv sync --extra dev          # + pytest, for running the test suite
uv sync --extra agents       # + AgentHandler (a2a-sdk, pydantic)
uv sync --extra parquet      # + ParquetSink (pyarrow)
uv sync --extra kafka        # + KafkaSink (confluent-kafka)
uv sync --extra db           # + DBSink (SQLAlchemy async engine)
```

Extras are additive -- pass as many `--extra` flags as you need. The core
package (`pipeloom.core`, `.messages`, `.exceptions`, `.stages`,
`.dataio.sources`, `.dataio.sinks`) never imports anything outside the
standard library; every other extra only matters if you import the module
that uses it.

## Quickstart

```python
import asyncio
from pipeloom import Handler, Pipeline, Record, RawRecord, Stage, step, run
from pipeloom.dataio.sinks import InMemorySink
from pipeloom.dataio.sources import IterableSource


class Uppercase(Handler):
    @step(Stage.TRANSFORM, consume=True)
    def go(self, message: RawRecord) -> Record:
        return Record({k: str(v).upper() for k, v in message.value.items()})


async def main() -> None:
    pipeline = Pipeline()
    sink = InMemorySink()
    pipeline.add_handler(Uppercase())
    pipeline.add_sink(sink)

    await run(IterableSource([{"name": "ada"}]), pipeline)
    print(sink.records)  # [{'name': 'ADA'}]


asyncio.run(main())
```

See `examples/sample_pipeline.py` for a runnable version (with validation,
error routing, multiple sinks, and the profiler), plus `.vscode/launch.json`
to run or debug it directly.

## Core concepts

- **`Message`** -- base class for anything flowing through a `Pipeline`.
  Built-in ones: `RawRecord` (raw input), `Record` (a dict of output fields),
  `ErrorMessage` (a failure, carrying the original message and exception),
  `Tick`/`Heartbeat` (periodic cadence), `Checkpoint`/`Shutdown` (end-of-run
  lifecycle).
- **`Handler`** -- a class grouping one or more `@step` methods.
- **`@step(stage, consume=False)`** -- marks a `Handler` method as a pipeline
  step. It must take `(self, message: SomeMessageSubclass)` and return
  `None`, a `Message`, or an `Iterable[Message]`. `consume=True` means the
  input message stops there; the default `consume=False` re-queues it so it
  keeps routing to later steps.
- **`Stage`** -- an ordered set of ints (`EXTRACT < VALIDATE < ENRICH < AGENT
  < TRANSFORM < LOAD < MONITOR`) fixing the order steps run in, regardless of
  registration order. Steps within one `Handler` must be declared in
  non-decreasing stage order.
- **`Pipeline`** -- flattens every registered `Handler`'s steps into one
  chain and routes messages through it by type. A step raising an exception
  turns it into an `ErrorMessage` that keeps flowing (failures travel as
  data); an `ErrorMessage` nothing handles becomes a fatal `PipelineFailed`
  via the built-in catch-all.
- **`run(source, pipeline, tick_interval=None)`** -- drives every record from
  a `Source` through a `Pipeline`, then emits a final `Checkpoint` and
  `Shutdown`. A `Handler` raising `PipelineStopped` ends the run early but
  successfully. Pass `tick_interval` (seconds) to also inject `Tick`
  messages between records, for monitoring Handlers (see below).

## Sinks: `add_handler` vs `add_sink`

A `Sink` is just a `Handler` whose steps write output somewhere. How you
register it changes its delivery guarantee:

- **`pipeline.add_handler(sink)`** -- normal routing. A message is delivered
  to the *first* step downstream that accepts its type; if that step
  `consume`s it (the built-in sinks do), no other handler ever sees that
  message. Fine for exactly one sink.
- **`pipeline.add_sink(sink)`** -- broadcast. *Every* registered sink whose
  step accepts a message's type receives it, so you can register as many
  sinks as you want and all of them see every record. A message handled by
  at least one sink is fully consumed (it never reaches `add_handler`-routed
  steps or the catch-all). If one sink raises, that failure becomes an
  `ErrorMessage` fed back into the pipeline and the remaining sinks still run
  -- one broken sink can't block the others.

```python
pipeline.add_sink(InMemorySink())
pipeline.add_sink(JSONLinesSink("out.jsonl"))
pipeline.add_sink(ParquetSink("out.parquet"))
# every Record and ErrorMessage now reaches all three
```

Use `add_handler` only when you truly want a single, ordered consumer (or
when writing a step that also needs to observe/transform, not just sink).

### Built-in sinks (`pipeloom.dataio.sinks`, no extra required)

| Sink | Writes to |
|---|---|
| `InMemorySink` | `.records` / `.errors` lists -- useful for tests |
| `JSONLinesSink(path, error_path=None)` | JSON Lines file; errors go to a lazily-opened `error_path` (default `path + ".errors"`) |
| `CSVSink(path)` | CSV file; header taken from the first `Record`'s keys, later records must match |

### Optional sinks (own extra + submodule each)

These live outside `pipeloom.dataio.sinks` specifically so importing the
core sinks module never pulls in their dependency -- import the submodule
you need.

| Sink | Extra | Needs |
|---|---|---|
| `pipeloom.dataio.parquet.ParquetSink(path, batch_size=1000)` | `parquet` | pyarrow |
| `pipeloom.dataio.kafka.KafkaSink(topic, producer_config=None, producer=None, key_fn=None)` | `kafka` | confluent-kafka |
| `pipeloom.dataio.db.DBSink(engine, table, batch_size=100)` | `db` | SQLAlchemy async engine + a driver (asyncpg/aiomysql/aiosqlite/...) |

All three batch records and require an explicit `close()` (or `await
close()` for `DBSink`) to flush what's buffered -- call it after `run()`
returns, or from a `Shutdown`-handling step.

```python
from pipeloom.dataio.kafka import KafkaSink
from pipeloom.dataio.db import DBSink
from sqlalchemy.ext.asyncio import create_async_engine

pipeline.add_sink(KafkaSink("records", {"bootstrap.servers": "localhost:9092"}))

engine = create_async_engine("postgresql+asyncpg://...")
pipeline.add_sink(DBSink(engine, my_table))
```

## Sources (`pipeloom.dataio.sources`)

`IterableSource(records)` (in-memory, for tests), `JSONLinesSource(path)`,
`CSVSource(path)` -- all implement `Source.get_records() ->
AsyncIterator[RawRecord]`.

## Profiling

```python
profile = pipeline.get_profile_data()
print(pipeline.format_profile(profile))                    # timing/call-count summary
print(pipeline.format_profile(profile, blueprint=True))    # + types, consume flag, docstring, per-call ms
```

`blueprint=True` groups steps by `STAGE:` and shows each one's accepted/return
types and consume flag inline -- a static picture of the pipeline's shape
alongside the runtime numbers.

## Monitoring (`pipeloom.monitor`)

`RateMonitor` and `PipelineProfiler` are `Handler`s that react to `Tick`
(see `run`'s `tick_interval`) to log throughput and the profiling blueprint
periodically:

```python
from pipeloom.monitor import PipelineProfiler, RateMonitor

pipeline.add_handler(RateMonitor())
pipeline.add_handler(PipelineProfiler(pipeline))
await run(source, pipeline, tick_interval=10.0)
```

## Agents (`pipeloom.agents`, `agents` extra)

`AgentHandler(agent_url, retry_policy=None, output_schema=None)` is a
`Handler` whose single `@step(Stage.AGENT)` sends `RawRecord.value` to a
remote AI agent over A2A, retries transient failures, and validates the
response against `output_schema` (a Pydantic model) before emitting a
`Record`. Retries have no idempotency key -- safe against idempotent agents,
risky otherwise.

## Development

```bash
uv run pytest -q                              # full suite
PYTHONPATH=src uv run python examples/sample_pipeline.py
```

VS Code users: `.vscode/launch.json` has configs to run/debug the sample
pipeline, the current file, or the test suite under the debugger.
