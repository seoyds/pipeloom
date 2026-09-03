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
    pipeline.add_handler(Uppercase())
    sink = pipeline.add_sink(sink_type="memory", sink_trigger=Record)

    await run(IterableSource([{"name": "ada"}]), pipeline)
    print(sink.received)  # [{'name': 'ADA'}]


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

## Batching

A pipeline can shrink and expand as it flows -- e.g. a step calls an API in
bulk and gets one batched response back, but everything downstream needs to
work row by row; a later step needs to call a *different* bulk API, so those
rows need re-grouping into batches again. Two different mechanisms, one for
each direction:

- **Batch -> items ("shrink")** -- no special class needed. An ordinary
  `@step` returning `Iterable[Message]` (a list, or a generator) fans out:
  each yielded `Message` gets routed downstream independently.

  ```python
  @step(Stage.VALIDATE, consume=True)
  def explode(self, message: RawRecord) -> Iterable[Record]:
      for item in message.value["items"]:
          yield Record(item)
  ```

- **Items -> batch ("expand")** -- needs state carried across many `@step`
  calls, so it uses `Batcher`: subclass it, implement `make_batch(items)`,
  then add your own `@step` methods that call `self.accumulate(message)` for
  the item type (`consume=True`) and `self.flush()` for `Checkpoint` (and
  `Shutdown`, as a safety net for an early `PipelineStopped`, which skips
  `Checkpoint`) so a trailing partial batch is never silently dropped:

  ```python
  class RecordBatcher(Batcher):
      def make_batch(self, items: List[Message]) -> RecordBatch:
          return RecordBatch(list(items))

      @step(Stage.TRANSFORM, consume=True)
      def on_item(self, message: Record) -> Optional[RecordBatch]:
          return self.accumulate(message)

      @step(Stage.TRANSFORM, consume=False)
      def on_checkpoint(self, message: Checkpoint) -> Optional[RecordBatch]:
          return self.flush()

      @step(Stage.TRANSFORM, consume=False)
      def on_shutdown(self, message: Shutdown) -> Optional[RecordBatch]:
          return self.flush()
  ```

See `examples/sample_pipeline_batching.py` for a runnable version combining
both directions, plus `.vscode/launch.json` to run or debug it directly.

## Sinks

A sink is a plain class with one `write(message)` method -- no `@step`, no
notion of "record" vs "error" of its own. Two ways to register one with
`Pipeline.add_sink`:

- **`add_sink(sink_type=..., sink_params=..., sink_trigger=...)`** -- builds
  a sink from the built-in registry (table below), wired to fire on exactly
  one message type. `sink_trigger` is required and never defaulted -- e.g.
  `Record` or `ErrorMessage` -- since it's what actually activates the sink.
  Returns the constructed sink so you can read its state or call `close()`.
- **`add_sink(some_handler)`** -- a custom `Handler` you wrote yourself, for
  cases the registry doesn't cover; its own `@step`s decide what it reacts
  to (and can react to more than one type, unlike the config path).

Either way, `add_sink` is a **broadcast**, unlike `add_handler`'s
single-path routing (where only the first matching step ever sees a given
message): *every* registered sink whose trigger matches a message's type
receives it, so you can register as many sinks as you want and all of them
see every record. A message handled by at least one sink is fully consumed
(it never reaches `add_handler`-routed steps or the catch-all). If one sink
raises, that failure becomes an `ErrorMessage` fed back into the pipeline
and the remaining sinks still run -- one broken sink can't block the others.

A sink only ever fires on the one `sink_trigger` it was registered with. To
also persist `ErrorMessage`s, register a **second, separate** `add_sink`
call with `sink_trigger=ErrorMessage` -- same sink_type or a different one,
your choice:

```python
pipeline.add_sink(sink_type="memory", sink_trigger=Record)
pipeline.add_sink(sink_type="json", sink_params={"path": "out.jsonl"}, sink_trigger=Record)
pipeline.add_sink(sink_type="json", sink_params={"path": "errors.jsonl"},
                   sink_trigger=ErrorMessage)  # a separate sink, separate trigger
```

### Built-in sink types (`pipeloom.dataio.sinks`, no extra required)

| sink_type | sink_params | Writes to |
|---|---|---|
| `"memory"` | *(none)* | `.received` list on the returned `InMemorySink` -- useful for tests |
| `"json"` | `path` | JSON Lines file; call `close()` when done |
| `"csv"` | `path` | CSV file; header taken from the first row's keys, later rows must match; call `close()` when done |

### Optional sink types (own extra + submodule each)

These live outside `pipeloom.dataio.sinks` specifically so importing the
core sinks module never pulls in their dependency -- **importing the
submodule is what registers the sink_type**, so `import
pipeloom.dataio.parquet` (even unused directly) before calling
`add_sink(sink_type="parquet", ...)`.

| sink_type | Extra | sink_params | Needs |
|---|---|---|---|
| `"parquet"` | `parquet` | `path`, `batch_size=1000` | pyarrow |
| `"kafka"` | `kafka` | `topic`, `producer_config` (or `producer=` for a test double), `key_fn=None` | confluent-kafka |
| `"rdb"` | `db` | `table`, plus either `engine=` (pre-built) or `db_type` ("postgres"/"mysql"/"sqlite") + `host`/`port`/`user`/`password`/`database`, `batch_size=100` | SQLAlchemy async engine + a matching driver (asyncpg/aiomysql/aiosqlite) |

All three batch rows and require an explicit `close()` (or `await close()`
for `"rdb"`, since it's async) to flush what's buffered -- call it after
`run()` returns, or from a `Shutdown`-handling step. A database without a
native asyncio driver (e.g. Snowflake) isn't in `db_type`'s table -- build
the engine yourself and pass it as `engine=`.

```python
import pipeloom.dataio.kafka  # noqa: F401 -- registers sink_type="kafka"
import pipeloom.dataio.db     # noqa: F401 -- registers sink_type="rdb"

pipeline.add_sink(sink_type="kafka", sink_trigger=Record, sink_params={
    "topic": "records",
    "producer_config": {"bootstrap.servers": "localhost:9092"},
})

pipeline.add_sink(sink_type="rdb", sink_trigger=Record, sink_params={
    "table": my_table,
    "db_type": "postgres",
    "host": "localhost", "port": 5432, "user": "app", "password": "...", "database": "app",
})
```

## Sources (`pipeloom.dataio.sources`)

`IterableSource(records)` (in-memory, for tests), `JSONLinesSource(path)`,
`CSVSource(path)` -- all implement `Source.get_records() ->
AsyncIterator[RawRecord]`.

## Service (request/response) (`pipeloom.service`)

`run()` drives a `Pipeline` end-to-end over a `Source` until exhausted.
`PipelineService` instead lets you embed an existing `Pipeline` behind a
request/response call -- feed it one row, get back exactly what that row
produced:

```python
from pipeloom.service import PipelineService

service = PipelineService(pipeline)
records = await service.process_one({"name": "ada", "role": "engineer"})
# records: every Record/ErrorMessage that row produced, in emission order
```

- **Non-blocking front door, serial execution**: `process_one` calls never
  block on each other -- each row is queued immediately -- but internally a
  single worker loop processes one row at a time through the pipeline's
  step chain, so no two rows' steps ever overlap. This preserves every
  `Handler`'s single-consumer assumption (safe mutable instance state)
  exactly as `run()` does; it does not add pipeline-level concurrency.
- **Row-scoped fatal errors**: a `PipelineStopped` or `PipelineFailed`
  raised while processing a submitted row fails only that row's
  `process_one` call -- the service keeps serving rows submitted
  afterward. This diverges from `run()`, where the same exceptions end the
  whole run.
- **No shutdown needed**: a persistent service has no natural
  "exhausted" point, so there's no equivalent of `run()`'s final
  `Checkpoint`/`Shutdown` -- keep calling `process_one` for as long as rows
  arrive.
- Internally, `PipelineService` registers its own `add_sink`-based
  collector to correlate results with the submitting row; any other sinks
  already registered on the pipeline keep receiving every message exactly
  as they would under `run()`.

See `examples/sample_pipeline_service.py` for a runnable version (errors as
data vs. a row-scoped fatal error, a coexisting sink, and concurrent
submissions still processed serially).

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

See `examples/sample_pipeline_agent.py` for a runnable version (a local
step composed with an agent-backed step, output schema validation, and a
non-retryable agent failure routed to an ErrorMessage) -- it fakes the A2A
client so it runs with no network dependency; point `agent_url` at a real
agent and drop `client=` to use it for real.

## Development

```bash
uv run pytest -q                              # full suite
PYTHONPATH=src uv run python examples/sample_pipeline.py
PYTHONPATH=src uv run python examples/sample_pipeline_batching.py
PYTHONPATH=src uv run python examples/sample_pipeline_service.py
PYTHONPATH=src uv run python examples/sample_pipeline_agent.py
```

VS Code users: `.vscode/launch.json` has configs to run/debug any of the
sample pipelines, the current file, or the test suite under the debugger.
