# pipeline-core Specification

## Purpose

The core execution model: typed `Message`s flow through `Handler`-declared
`@step`s, which `Pipeline` flattens into one stage-ordered chain and drives
by type. A step's exception becomes an `ErrorMessage` that keeps flowing
instead of crashing the run, so failures travel as data. `run()` drives a
`Source`'s records through a `Pipeline` end to end.

## Requirements

### Requirement: Built-in Message Types
The system SHALL provide a `Message` base class and a fixed set of built-in
subclasses covering raw input, successful output, failure, cadence, and
end-of-run lifecycle.

#### Scenario: RawRecord carries the raw input
- **WHEN** a `Source` yields input
- **THEN** it is wrapped as `RawRecord(value=<dict>, time=<float>)`

#### Scenario: Record behaves like a dict
- **WHEN** a step returns `Record({"name": "ADA"})`
- **THEN** the resulting message supports dict operations (`dict(record)`, key access, etc.)

#### Scenario: ErrorMessage carries the failure context
- **WHEN** a step raises an exception while processing message `m`
- **THEN** the pipeline constructs `ErrorMessage(message=m, exception=<exc>, reason=<exc.reason if present else None>)`

#### Scenario: Lifecycle and cadence messages
- **WHEN** a run is driven via `run()`
- **THEN** `Tick`/`Heartbeat` messages may appear for periodic cadence, and `Checkpoint`/`Shutdown` mark end-of-run

### Requirement: Step Declaration
A `Handler` method decorated with `@step(stage, consume=False)` SHALL be
validated at decoration time and, if valid, registered as a pipeline step.

#### Scenario: Missing return annotation is rejected
- **WHEN** a method decorated with `@step` has no annotated return type
- **THEN** `AssertionError` is raised at decoration time

#### Scenario: Wrong argument count is rejected
- **WHEN** a decorated method does not take exactly `(self, message)`
- **THEN** `AssertionError` is raised at decoration time

#### Scenario: Non-Message argument annotation is rejected
- **WHEN** a decorated method's `message` argument is not annotated with a `Message` subclass
- **THEN** `AssertionError` is raised at decoration time

#### Scenario: Unsupported return type is rejected
- **WHEN** a decorated method's return annotation is not `None`, a `Message` subclass, an `Iterable`/`List` of one, or a `Union` of such types
- **THEN** `AssertionError` is raised at decoration time

#### Scenario: consume flag controls requeueing
- **WHEN** a step is declared with `consume=True`
- **THEN** the input message it processed is not re-queued for further routing
- **WHEN** a step is declared with the default `consume=False`
- **THEN** the input message is re-queued and continues routing to later steps

### Requirement: Stage Ordering
Steps SHALL run in a fixed stage order (`EXTRACT < VALIDATE < ENRICH < AGENT
< TRANSFORM < LOAD < MONITOR`), independent of registration order across
`Handler`s, but steps within a single `Handler` must already be declared in
non-decreasing stage order.

#### Scenario: Cross-handler ordering follows stage, not registration order
- **WHEN** `Handler` B (an earlier stage) is registered with `add_handler` after `Handler` A (a later stage)
- **THEN** B's steps still run before A's steps for a matching message

#### Scenario: Out-of-order steps within one Handler are rejected
- **WHEN** a `Handler` declares a step at an earlier stage after one at a later stage
- **THEN** `Handler.get_steps()` raises `AssertionError`

#### Scenario: A Handler with no steps is rejected
- **WHEN** `Handler.get_steps()` is called on a `Handler` with no `@step` methods
- **THEN** `AssertionError` is raised

### Requirement: Handler Registration
`Pipeline.add_handler`/`remove_handler` SHALL manage the set of `Handler`s
whose steps are flattened into the routing chain.

#### Scenario: Registering the same Handler twice is rejected
- **WHEN** `add_handler(h)` is called and `h` is already registered
- **THEN** `ValueError` is raised

#### Scenario: Registration invalidates the cached chain
- **WHEN** `add_handler` or `remove_handler` is called
- **THEN** the next `process()` or `get_profile_data()` call rebuilds the routing chain

### Requirement: Message Routing
`Pipeline.process(message)` SHALL route a message by type through the
flattened chain, matching the first step (after the current position) whose
accepted type the message is an instance of.

#### Scenario: First matching step wins
- **WHEN** a message of type `T` is routed through `add_handler`-registered steps
- **THEN** it is handed to the first downstream step (in stage order) whose accepted type `T` is a subclass of

#### Scenario: No matching step raises RoutingError
- **WHEN** no downstream step accepts a message's type
- **THEN** `RoutingError` is raised

#### Scenario: A sink match takes precedence over routed steps
- **WHEN** a message's type matches at least one registered sink's trigger (see [[sinks]])
- **THEN** the message is delivered to every matching sink and never reaches `add_handler`-routed steps or the built-in catch-all

### Requirement: Error-as-Data
An exception raised inside a step SHALL NOT crash the run; it is converted
into an `ErrorMessage` that continues routing from the failing step.

#### Scenario: A step exception becomes a routable ErrorMessage
- **WHEN** a step raises any `Exception` other than `PipelineStopped`/`PipelineFailed`
- **THEN** an `ErrorMessage(message=<input>, exception=<exc>, reason=getattr(exc, "reason", None))` is pushed back into routing from the failing step

#### Scenario: PipelineStopped/PipelineFailed propagate directly
- **WHEN** a step raises `PipelineStopped` or `PipelineFailed`
- **THEN** the exception propagates out of `process()` uncaught and unconverted

#### Scenario: An unhandled ErrorMessage is fatal
- **WHEN** an `ErrorMessage` reaches the end of the chain without being consumed by any step or sink
- **THEN** the built-in catch-all raises `PipelineFailed` with the original exception's `repr()` in its message

#### Scenario: Any other unrouted message is silently dropped
- **WHEN** a non-`ErrorMessage` message reaches the end of the chain without being consumed
- **THEN** the built-in catch-all consumes it and produces no error

### Requirement: Emitted Message Handling
A step's return value SHALL determine what (if anything) is emitted next.

#### Scenario: None emits nothing
- **WHEN** a step returns `None`
- **THEN** no new message is queued

#### Scenario: A single Message is emitted
- **WHEN** a step returns a `Message` instance
- **THEN** that message is queued for routing and counted once in the step's emitted count

#### Scenario: An iterable of Messages is emitted in order
- **WHEN** a step returns an iterable of `Message` instances
- **THEN** each is queued for routing (processed in the order returned) and counted in the step's emitted count

#### Scenario: A non-Message item in an iterable is rejected
- **WHEN** a step returns an iterable containing a non-`Message` value
- **THEN** `PipelineError` is raised

#### Scenario: An invalid return value is rejected
- **WHEN** a step returns a value that is neither `None`, a `Message`, nor an iterable
- **THEN** `PipelineError` is raised

### Requirement: run() Drives a Source Through a Pipeline
`run(source, pipeline, tick_interval=None)` SHALL drive every record from a
`Source` through a `Pipeline`, then emit a final `Checkpoint` and `Shutdown`.

#### Scenario: Every source record is processed in order
- **WHEN** `run(source, pipeline)` is awaited
- **THEN** every `RawRecord` yielded by `source.get_records()` is passed to `pipeline.process()` in yield order

#### Scenario: Normal completion emits Checkpoint then Shutdown
- **WHEN** the source is exhausted without an early stop
- **THEN** `pipeline.process(Checkpoint())` runs, followed by `pipeline.process(Shutdown())`

#### Scenario: PipelineStopped ends the run early but successfully
- **WHEN** a `Handler` raises `PipelineStopped` while processing any message (including a `Tick` or a record)
- **THEN** the run ends without processing further records or emitting `Checkpoint`, but `Shutdown` is still emitted (itself wrapped so a `PipelineStopped` raised while handling `Shutdown` does not propagate)

#### Scenario: tick_interval injects Tick between records
- **WHEN** `tick_interval` seconds or more have elapsed since the last `Tick` (checked once per incoming record, before that record is processed)
- **THEN** `pipeline.process(Tick())` runs before the record is processed, and the elapsed-time clock resets
