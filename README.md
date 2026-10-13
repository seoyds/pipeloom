# pipeloom

**Build large data pipelines out of small parts you can swap one at a time.**

pipeloom is an async, typed pipeline framework for Python. You write small
`Handler` classes. Each `@step` method declares two things: the **stage** where
it runs and the **message type** it accepts. pipeloom builds the pipeline from
those declarations. You register handlers, but you never wire them to each
other, and no handler ever refers to another one. A step can be plain Python or a remote AI agent reached
over A2A. The core has zero runtime dependencies.

```python
class Uppercase(Handler):
    @step(Stage.TRANSFORM, consume=True)
    def go(self, message: RawRecord) -> Record:
        return Record({k: str(v).upper() for k, v in message.value.items()})
```

That signature is the whole contract: *runs in TRANSFORM, takes a `RawRecord`,
emits a `Record`*. `consume=True` means the incoming `RawRecord` stops here.
Without it, the original message also carries on to later steps. Nothing else
in the pipeline needs to know this class exists.

---

## Why pipeloom exists

### A pipeline that grows becomes a machine

A small pipeline is a script: read, clean, write. You can hold all of it in
your head. Production pipelines don't stay small. Each new data source,
validation rule, enrichment call, model, sink or monitor adds a part. After a
year you have something like an engine room. Power comes in at one end, gets
passed through dozens of gears, shafts and belts, and comes out as rows in a
warehouse.

Google's engineers described this exact decay in *Hidden Technical Debt in
Machine Learning Systems* (Sculley et al., NeurIPS 2015). They called the
result a **pipeline jungle**. Pipelines "evolve organically, as new signals are
identified and new information sources added incrementally," until they become
"a jungle of scrapes, joins, and sampling steps." They also named the
consequence: "Testing such pipelines often requires expensive end-to-end
integration tests." Their remedy is the one this library is built on: "strong
abstraction boundaries using encapsulation and modular design help create
maintainable code in which it is easy to make isolated changes and
improvements."

### You never repair a machine as a whole

Look at how big machines are actually maintained. When something fails on an
aircraft at the gate, the mechanic does not study the whole aircraft. Avionics
are built as **line-replaceable units** (LRUs): self-contained boxes, each in a
known bay, each plugged in through a standard connector. The US military
specification defines an LRU as an item "removed and replaced at the field
level to restore the end item to an operational ready condition." The mechanic
finds the faulty unit, pulls it, slots in a spare, and the aircraft flies
again. The broken unit is repaired later, in a workshop, away from the
aircraft.

That works because of three properties, and you need all three:

1. **Known position.** You know where the unit sits, so you can find it.
2. **Standard interface.** You know exactly what goes into it and what comes
   out, so a replacement only has to honour the connector.
3. **Isolation.** Pulling one unit doesn't take the others apart, and one
   unit failing doesn't stop the rest.

The full assembly drawing of the machine matters, but you can't work from it.
At that scale every detail disappears. Real work happens one part at a time.
David Parnas listed the same benefit in his 1972 paper on decomposing systems
into modules: "it should be possible to study the system one module at a
time" and "to make drastic changes to one module without a need to change
others."

### In pipeloom, every handler is a replaceable unit

pipeloom gives each handler those three properties through ordinary Python:

| LRU property | In pipeloom | Where it lives |
|---|---|---|
| Known position (the bay) | The **stage**: `EXTRACT < VALIDATE < ENRICH < AGENT < TRANSFORM < LOAD < MONITOR` | `@step(Stage.ENRICH)` |
| Standard connector | The **message types** it accepts and emits | `def f(self, m: RawRecord) -> Record` |
| No shared wiring | Handlers never import, call, or name each other. They only share message types. | Routing happens by type |
| Failure stays local | An exception becomes an `ErrorMessage` that keeps flowing, so one bad record doesn't crash the run | Built-in; you only add an `ErrorMessage` sink or step to collect them |

Stages are plain integers, so you can add your own between the built-in ones
(for example `@step(15)` for a dedupe pass between VALIDATE and ENRICH).

Read any handler on its own and you know **where it runs** (its stage),
**what it touches** (its input type), and **what it hands on** (its return
type). You don't need to open any other file to learn that.

### The types are the wiring

Most pipeline tools make you draw the graph: `extract >> clean >> load` in
Airflow, `p | A | B` in Beam, `a | b` in a Unix shell. That graph is a second
copy of the truth, and you have to keep it in sync with the code. Every new
step means editing the wiring around it, so changing one part means touching
its neighbours.

pipeloom works more like a postal sorting office. A letter is routed by the
address written on it, not by the sender phoning ahead to every depot on the
way. Here, a message is routed by **its type**. pipeloom flattens every
handler's steps into one chain and sorts it by stage. When a step emits a
message, it goes to the **next step downstream** whose annotation accepts that
type. Messages only ever move forward, which is what makes the stage a real
position and not just a label. Adding a new delivery route doesn't move any
of the existing ones.

Sinks work slightly differently, closer to hormones and receptors. A hormone
goes out into the bloodstream, and every cell with the matching receptor
responds. Register a sink for `Record` and it receives every `Record`,
alongside every other `Record` sink.

### What a change actually touches

Here is a three-part order pipeline. Each part declares what it takes and what
it gives:

```python
class Order(Message, dict): ...     # a parsed order
class Labeled(Message, dict): ...   # an order with a category attached

class ParseOrder(Handler):
    @step(Stage.VALIDATE, consume=True)
    def parse(self, message: RawRecord) -> Order:
        return Order(message.value)

class RuleLabeler(Handler):
    @step(Stage.ENRICH, consume=True)
    def label(self, message: Order) -> Labeled:
        return Labeled(message, category="bulk" if message["qty"] > 100 else "retail")

class Finalize(Handler):
    @step(Stage.TRANSFORM, consume=True)
    def finish(self, message: Labeled) -> Record:
        return Record(message)

pipeline.add_handler(ParseOrder())
pipeline.add_handler(RuleLabeler())
pipeline.add_handler(Finalize())
```

Now the rules aren't good enough and you want an LLM to do the labelling. You
write one new part with the same connector, `Order -> Labeled`:

```python
class LLMLabeler(Handler):
    @step(Stage.AGENT, consume=True)
    async def label(self, message: Order) -> Labeled:
        category = await classify_with_llm(message)   # your model call
        return Labeled(message, category=category)
```

Then you change **one line**: `add_handler(RuleLabeler())` becomes
`add_handler(LLMLabeler())`. `ParseOrder` and `Finalize` don't change, and
they don't know anything happened. The new part even runs at a different stage
(AGENT instead of ENRICH), and routing still works, because the types still
line up.

See `examples/sample_pipeline_swap.py` for a runnable version that runs both
labellers through the same `ParseOrder` and `Finalize` instances and prints
the difference.

The same goes for other changes:

| You want to... | You touch | You don't touch |
|---|---|---|
| Add a validation rule | One new `Handler` at `Stage.VALIDATE`, plus one `add_handler` line | Any existing handler |
| Remove an enrichment step | The `add_handler` line (or `pipeline.remove_handler(h)`) | Upstream or downstream steps, as long as the types still line up |
| Call a remote A2A agent instead of local code | Register the built-in `AgentHandler` (`RawRecord -> Record`) in place of a local step with the same types | Everything else |
| Send output to Kafka as well as Postgres | One `add_sink(sink_type="kafka", ...)` | The other sinks, and every handler |
| Collect a new kind of failure | An `ErrorMessage` sink, or an `ErrorMessage` step at a later stage than the failure | The step that failed. It just raises. |
| Find the slow or misbehaving part | Read the profiler blueprint (below) | Nothing |

Because each handler is self-contained, you can test it as a plain class:

```python
assert Uppercase().go(RawRecord({"name": "ada"}, time=0)) == {"name": "ADA"}
```

You don't need a staging environment to check that one gear turns.

### The whole-machine view, when you need it

Local views are for making changes. Sometimes you still need the overview: to
find the part that's slow, or to see where a message type goes. pipeloom
**generates** that view from the code, so it can't drift out of date:

```python
print(pipeline.format_profile(pipeline.get_profile_data(), blueprint=True))
```

```
STAGE: VALIDATE - 10
    FailOnEmpty.go(RawRecord)                  consumed  ---> RawRecord      2 calls ->  2 emitted
STAGE: TRANSFORM - 40
    UppercaseTransform.uppercase(RawRecord)    consumed  ---> Uppercased     2 calls ->  2 emitted
    UppercaseTransform.lowercase(Uppercased)   consumed  ---> Record         2 calls ->  2 emitted
STAGE: LOAD - 50
    InMemorySink.write(Record)                 consumed  ---> None           2 calls ->  0 emitted
    JSONLinesSink.write(Record)                consumed  ---> None           2 calls ->  0 emitted
    InMemorySink.write(ErrorMessage)           consumed  ---> None           1 calls ->  0 emitted
```

*(Trimmed from `examples/sample_pipeline.py`. The real output also shows
timing, share of total time, and ms per call for each step.)*

Each line is one part: its stage, what goes in, what comes out, and how hard
it's working. It's the assembly drawing and the parts list in one, rebuilt
every time you ask for it.

### How it compares

pipeloom isn't the first tool to infer a pipeline's graph instead of having
you wire it. The difference is **what** it infers the graph from:

| Tool | How steps are connected |
|---|---|
| Airflow, Beam, Unix pipes | You wire the graph explicitly (`>>`, `\|`) |
| Dagster assets | Inferred from function **parameter names** |
| Kedro | Inferred from dataset **name strings** in the catalog |
| EIP content-based router | A central router that knows every destination |
| **pipeloom** | Inferred from **type annotations** plus a stage order. No central router, no name matching, and subclass-aware. |

Types are something your editor, type checker, and tests already understand.
You can jump to a type's definition, find every handler that accepts it, and
rename it safely with ordinary refactoring tools. Routing is also
subclass-aware: a step that accepts `Order` also receives a `PriorityOrder`.

pipeloom is also a **record-at-a-time, in-process** framework. It isn't a
scheduler or orchestrator like Airflow or Dagster, and it doesn't replace
them. It's the engine you'd run *inside* one of their tasks, or behind an API
with `PipelineService`.

### Honest trade-offs

The same design choices that make changes local have costs:

- **The global flow isn't written down in one place.** That's the point of
  implicit wiring, and also its weakness. Martin Fowler notes the same about
  event-driven systems: the flow "is not explicit in any program text." The
  profiler blueprint is the answer. Use it, and consider checking it into
  review for large pipelines.
- **Types are the shared contract.** Changing a handler's internals is local.
  Changing a message type is not, because every handler that accepts or emits
  it is affected. Treat message types like a public API: add new types rather
  than mutating existing ones.
- **First match wins.** A new step that accepts a broad type (say, `Message`)
  at an early stage will capture messages before more specific steps further
  down. Prefer narrow, purpose-made types. The blueprint shows exactly who
  catches what.
- **Within one stage, registration order breaks ties.** Two steps at the same
  stage that accept the same type run in the order you called `add_handler`.
  If order matters, give them different stages.
- **Errors only travel forward.** An `ErrorMessage` continues downstream from
  the step that failed. A step that handles errors must sit at a later stage,
  or be registered as an `ErrorMessage` sink, which catches errors from
  anywhere.
- **Sinks match globally.** A sink registered for `Record` captures *every*
  `Record`, including one emitted halfway through a multi-step handler. Use an
  intermediate type (see `Uppercased` in `examples/sample_pipeline.py`) for
  values that aren't final yet.
- **Unrouted messages are dropped.** A message type that no step accepts is
  quietly discarded by the built-in catch-all. An *unhandled* `ErrorMessage`
  is never dropped: it stops the run with `PipelineFailed`.
- **It's a line, not an arbitrary graph.** Steps run in stage order. Fan-out
  (return an iterable) and re-batching (`Batcher`) are built in, but complex
  joins across independent streams belong in your database or orchestrator.

If your pipeline is three steps that will never change, a plain function is
simpler. pipeloom pays off when the pipeline keeps growing, several people
change it, and you want each change to stay small.

### Why "pipeloom"?

A *pipe* carries the flow. A *loom* is the machine that weaves many separate
threads into one fabric. On a loom, every warp thread runs through its own
fixed heddle. When one breaks, the weaver mends that thread and carries on,
without rebuilding the machine. pipeloom treats each handler the same way.

The loom is also the engine's ancestor. Babbage borrowed the Jacquard loom's
punched cards for his Analytical Engine, and Ada Lovelace wrote that it
"weaves algebraic patterns just as the Jacquard-loom weaves flowers and
leaves."

---

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
  < TRANSFORM < LOAD < MONITOR`) fixing the order steps run in across
  handlers, regardless of registration order. Within the same stage,
  registration order breaks ties. Steps within one `Handler` must be declared
  in non-decreasing stage order.
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
PYTHONPATH=src uv run python examples/sample_pipeline_swap.py
PYTHONPATH=src uv run python examples/sample_pipeline_service.py
PYTHONPATH=src uv run python examples/sample_pipeline_agent.py
```

VS Code users: `.vscode/launch.json` has configs to run/debug any of the
sample pipelines, the current file, or the test suite under the debugger.

## Further reading

- D. Sculley et al., [*Hidden Technical Debt in Machine Learning Systems*](https://proceedings.neurips.cc/paper_files/paper/2015/file/86df7dcfd896fcaf2674f757a2463eba-Paper.pdf), NeurIPS 2015. Covers pipeline jungles and glue code.
- D. L. Parnas, [*On the Criteria To Be Used in Decomposing Systems into Modules*](https://wstomv.win.tue.nl/edu/2ip30/references/criteria_for_modularization.pdf), CACM 1972.
- [Line-replaceable unit](https://en.wikipedia.org/wiki/Line-replaceable_unit). The maintenance model behind the analogy.
- Hohpe & Woolf, *Enterprise Integration Patterns*: [Content-Based Router](https://www.enterpriseintegrationpatterns.com/patterns/messaging/ContentBasedRouter.html), [Invalid Message Channel](https://www.enterpriseintegrationpatterns.com/patterns/messaging/InvalidMessageChannel.html).
- M. Fowler, [*What do you mean by "Event-Driven"?*](https://martinfowler.com/articles/201701-event-driven.html). Covers the cost of implicit flow.
