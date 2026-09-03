## Why

Today pipeloom only drives a `Pipeline` via `run()`, which pulls records from
a `Source` until exhausted. There is no way to embed a `Pipeline` behind a
request/response call — feed it one row, get back what it produced for that
row — so anyone wanting pipeloom-shaped validation/transform/agent logic
inside a synchronous caller (an API handler, a worker task, a test) has to
either bypass `Pipeline` entirely or hand-roll their own request/response
correlation on top of it.

## What Changes

- Add a `PipelineService` wrapper: given a pre-built `Pipeline`, exposes an
  async `process_one(value: dict) -> list[Message]` call that returns every
  `Record`/`ErrorMessage` the pipeline produced for that one row, in
  emission order.
- Internally, `PipelineService` runs a single serial worker loop fed by an
  `asyncio.Queue`, so `process_one` calls never block each other at the
  call site (non-blocking front door) while the pipeline's step chain still
  only ever processes one row at a time (no pipeline-level concurrency) —
  preserving every existing `Handler`'s assumption that its own mutable
  state is never accessed by two rows at once.
- Add a `CorrelatedCollector` sink (a `Sink`-based `Handler`, registered via
  `add_sink`) that captures `Record`/`ErrorMessage` messages for the
  in-flight row without disturbing any other sinks already registered on
  the pipeline (`add_sink`'s existing broadcast model is unchanged).
- Define request/response fatal-error semantics distinct from `run()`: a
  `PipelineStopped`/`PipelineFailed` raised while processing one row is
  scoped to that row (its `process_one` call raises the exception) and does
  **not** stop the worker loop — the service keeps serving subsequent rows.
  This is a deliberate divergence from `run()`, where the same exceptions
  end the whole run; `run()`'s behavior is unchanged.
- No explicit shutdown/lifecycle method for `PipelineService` — a persistent
  service has no natural "exhausted" point, so nothing to drain or close by
  default (out of scope; `run()`'s existing `Checkpoint`/`Shutdown` handling
  already covers the batch/streaming case).

## Capabilities

### New Capabilities
- `pipeline-service`: request/response wrapper around an existing
  `Pipeline` — non-blocking submission, serial execution, per-row result
  collection, and row-scoped fatal-error handling.

### Modified Capabilities
(none — this is purely additive; `Pipeline`, `Handler`, `add_sink`, and
`run()` behavior are all unchanged)

## Impact

- New module under `src/pipeloom/` (e.g. `pipeloom/service.py`); no changes
  to `pipeloom.core`, `pipeloom.pipeline`, `pipeloom.messages`, or
  `pipeloom.dataio`.
- No new runtime dependencies — built entirely on `asyncio` primitives
  already implied by the existing async `Pipeline.process`/`run`.
- Existing `Handler`/sink authors are unaffected unless they opt into
  `PipelineService`; pipelines driven by `run()` see no behavior change.
