# sinks Specification

## Purpose

A sink is a plain class with one `write(message)` method (no `@step`, no
notion of "record" vs "error" of its own). `Pipeline.add_sink` registers
sinks as a **broadcast**: every registered sink whose trigger matches a
message's type receives it, unlike `add_handler`'s single-path routing. A
message handled by at least one sink is fully consumed by [[pipeline-core]]'s
routing.

## Requirements

### Requirement: add_sink Custom Handler Path
`add_sink(handler)` SHALL register a caller-written `Handler` as a sink,
whose own `@step` methods decide what message type(s) it reacts to.

#### Scenario: A custom Handler is registered as a sink
- **WHEN** `add_sink(my_handler)` is called with a `Handler` instance
- **THEN** `my_handler`'s `@step`s participate in broadcast dispatch and the call returns `my_handler`

#### Scenario: Registering the same sink Handler twice is rejected
- **WHEN** `add_sink(h)` is called and `h` is already registered as a sink
- **THEN** `ValueError` is raised

#### Scenario: Passing both sink and sink_type is rejected
- **WHEN** `add_sink(handler, sink_type="memory")` is called with both a positional `sink` and `sink_type`
- **THEN** `ValueError` is raised

### Requirement: add_sink Config-Driven Path
`add_sink(sink_type=..., sink_params=..., sink_trigger=...)` SHALL construct
a sink from the built-in registry and wire it to fire on exactly one message
type.

#### Scenario: sink_trigger is required and never defaulted
- **WHEN** `add_sink(sink_type="json", sink_params={...})` is called without `sink_trigger`
- **THEN** `ValueError` is raised, and no sink is constructed or registered

#### Scenario: An unknown sink_type is rejected
- **WHEN** `add_sink(sink_type="does-not-exist", sink_trigger=Record)` is called
- **THEN** `ValueError` is raised, listing the currently registered `sink_type`s

#### Scenario: A valid config constructs and registers the sink
- **WHEN** `add_sink(sink_type="memory", sink_trigger=Record)` is called
- **THEN** the registry's factory for `"memory"` is invoked with `sink_params` (or no arguments if omitted), the resulting object is registered to fire on `Record`, and it is returned

### Requirement: Broadcast Dispatch
Every registered sink whose accepted type matches a message SHALL receive
that message; a message handled by at least one sink is fully consumed.

#### Scenario: All matching sinks see the same message
- **WHEN** two sinks are both registered with `sink_trigger=Record` (or two custom sink Handlers both accept `Record`)
- **THEN** both receive the same `Record` message

#### Scenario: A sink-consumed message never reaches add_handler routing
- **WHEN** a message's type matches at least one registered sink
- **THEN** it is not delivered to any `add_handler`-registered step or the built-in catch-all

#### Scenario: A failing sink does not block other sinks
- **WHEN** one matching sink's `write()` raises an exception
- **THEN** an `ErrorMessage` for that failure is fed back into routing, and every other matching sink still receives the original message

### Requirement: Single-Trigger Config Sinks
A sink registered via the `sink_type=` path SHALL fire only on the one
`sink_trigger` it was configured with.

#### Scenario: A second trigger requires a second registration
- **WHEN** both `Record` and `ErrorMessage` need to reach a sink
- **THEN** two separate `add_sink(sink_type=..., sink_trigger=...)` calls are required — one per trigger type

### Requirement: remove_sink
`Pipeline.remove_sink(sink)` SHALL unregister a previously registered sink,
identified by the object `add_sink` returned.

#### Scenario: Removing a registered sink
- **WHEN** `remove_sink(x)` is called where `x` is the return value of an earlier `add_sink` call
- **THEN** that sink no longer receives messages after the next rebuild

#### Scenario: Removing an unregistered sink is rejected
- **WHEN** `remove_sink(x)` is called with an object that was never registered via `add_sink`
- **THEN** `ValueError` is raised

### Requirement: sink_type Registry
`register_sink_type(name, factory)` SHALL make a sink implementation
available under `name` for the `sink_type=` config path; a `sink_type` is
only available once its defining module has been imported.

#### Scenario: Importing a submodule registers its sink_type
- **WHEN** `pipeloom.dataio.parquet` (or `.kafka`, `.db`) is imported, even without referencing it directly
- **THEN** its `sink_type` (`"parquet"`, `"kafka"`, `"rdb"` respectively) becomes usable via `add_sink(sink_type=...)`

#### Scenario: Core sinks never require an extra
- **WHEN** only `pipeloom.dataio.sinks` is imported (directly or transitively)
- **THEN** `"memory"`, `"json"`, and `"csv"` are registered with no dependency beyond the standard library

### Requirement: Message-to-Row Flattening
`row_from_message(message)` SHALL flatten a `Record` or `ErrorMessage` into
the plain-dict row shape every built-in sink writes.

#### Scenario: A Record flattens to its own fields
- **WHEN** `row_from_message(record)` is called on a `Record`
- **THEN** it returns `dict(record)`

#### Scenario: An ErrorMessage flattens to a fixed error shape
- **WHEN** `row_from_message(err)` is called on an `ErrorMessage`
- **THEN** it returns `{"message": repr(err.message), "exception": str(err.exception), "reason": err.reason}`

#### Scenario: An unsupported message type is rejected
- **WHEN** `row_from_message(msg)` is called on a message that is neither `Record` nor `ErrorMessage`
- **THEN** `TypeError` is raised

### Requirement: Built-in Sink — memory
`sink_type="memory"` (`InMemorySink`) SHALL collect every message it
receives, unflattened, in a `.received` list — for tests and inspection.

#### Scenario: Received messages are kept as-is
- **WHEN** an `InMemorySink` receives a `Record`
- **THEN** the original `Record` object (not a flattened row) is appended to `.received`

### Requirement: Built-in Sink — json
`sink_type="json"` (`JSONLinesSink`) SHALL write each message as one JSON
Lines row to `path`, flattened via `row_from_message`.

#### Scenario: Rows are appended as JSON Lines
- **WHEN** a `JSONLinesSink` receives messages and `close()` is called
- **THEN** `path` contains one JSON object per line, one per received message, in receipt order

### Requirement: Built-in Sink — csv
`sink_type="csv"` (`CSVSink`) SHALL write each message as one CSV row to
`path`, with the header taken from the first row's keys.

#### Scenario: Header is taken from the first row
- **WHEN** the first message is written
- **THEN** the CSV header is written from that row's keys before the row itself

#### Scenario: A later row with different keys is rejected
- **WHEN** a subsequent row's key set differs from the established header's key set
- **THEN** `ValueError` is raised

### Requirement: Optional Sink — parquet
`sink_type="parquet"` (`ParquetSink`, `parquet` extra) SHALL batch rows and
write them to a Parquet file at `path`, inferring schema from the first row.

#### Scenario: Schema is inferred from the first row
- **WHEN** the first message is written
- **THEN** the Parquet schema is inferred from that row's keys/value types

#### Scenario: A later row with different keys is rejected
- **WHEN** a subsequent row's key set differs from the inferred schema's field names
- **THEN** `ValueError` is raised

#### Scenario: Rows are buffered and flushed in batches
- **WHEN** the internal buffer reaches `batch_size` (default 1000) rows
- **THEN** a row group is written and the buffer is cleared

#### Scenario: close() finishes the file
- **WHEN** `close()` is called
- **THEN** any buffered rows are flushed and the Parquet writer is closed, producing a valid file

### Requirement: Optional Sink — kafka
`sink_type="kafka"` (`KafkaSink`, `kafka` extra) SHALL publish each message
as a JSON row to a Kafka topic via `confluent_kafka.Producer`.

#### Scenario: Either producer_config or producer is required
- **WHEN** `KafkaSink(topic=...)` is constructed with neither `producer_config` nor `producer`
- **THEN** `ValueError` is raised

#### Scenario: A delivery failure surfaces on a later call
- **WHEN** an earlier `produce()` call's delivery callback reports an error
- **THEN** the next `write()` (or `close()`) that services callbacks raises that error, which [[pipeline-core]] converts into an `ErrorMessage`

#### Scenario: key_fn derives the Kafka message key
- **WHEN** `key_fn` is provided
- **THEN** it is called with the flattened row to compute the Kafka message key for `produce()`

#### Scenario: close() flushes in-flight messages
- **WHEN** `close()` is called
- **THEN** the producer's `flush()` blocks until all queued messages are delivered or fail

### Requirement: Optional Sink — rdb (database)
`sink_type="rdb"` (`DBSink`, `db` extra) SHALL batch rows and insert them
into a `Table` via a SQLAlchemy async engine.

#### Scenario: Either engine or db_type is required
- **WHEN** `DBSink(table=...)` is constructed with neither `engine` nor `db_type`
- **THEN** `ValueError` is raised

#### Scenario: db_type builds a matching async engine
- **WHEN** `db_type` is one of `"postgres"`/`"postgresql"`/`"mysql"`/`"sqlite"` plus connection fields
- **THEN** an `AsyncEngine` is built using that database's async driver (asyncpg/aiomysql/aiosqlite)

#### Scenario: An unsupported db_type is rejected
- **WHEN** `db_type` is not one of the supported values
- **THEN** `ValueError` is raised, naming the supported values and suggesting passing a pre-built `engine`

#### Scenario: Rows are buffered and inserted in batches
- **WHEN** the internal buffer reaches `batch_size` (default 100) rows
- **THEN** the buffered rows are inserted into `table` in one `executemany`-style statement inside a transaction

#### Scenario: close() is async and flushes the remainder
- **WHEN** `await close()` is called
- **THEN** any buffered rows are flushed via one final insert
