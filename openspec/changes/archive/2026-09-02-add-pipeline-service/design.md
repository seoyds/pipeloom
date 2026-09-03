## Context

See proposal.md - Why. `Pipeline.process(message)` (`pipeloom.core`) is
already a single-message, awaitable, broadcast-routing primitive; `run()`
(`pipeloom.pipeline`) is the only existing driver, and it assumes one
serial consumer pulling from a `Source` until exhausted. Every `Handler` in
this codebase (`RateMonitor`, `AgentHandler`, user code) is written against
that single-consumer assumption — mutable instance state (`self._count`,
`self.metrics`) is safe today only because nothing calls a `Handler`'s
steps concurrently.

## Goals / Non-Goals

**Goals:**
- Let a caller submit one row to a pre-built `Pipeline` and get back what
  that row produced, without blocking on other in-flight submissions.
- Preserve every existing `Handler`'s single-consumer assumption — no two
  rows' step-chain execution overlaps, ever.
- Zero changes to `pipeloom.core`, `pipeloom.messages`, `pipeloom.pipeline`,
  or `pipeloom.dataio` — purely additive.

**Non-Goals:**
- True pipeline-level concurrency (multiple rows executing through the step
  chain at once). Explicitly rejected — see Decisions.
- An HTTP/gRPC/etc. transport layer. This design produces a Python-level
  async call; wrapping it in a transport is a separate, later concern.
- A shutdown/drain lifecycle for the service (see proposal.md - out of
  scope; there is no natural end to a persistent service's input).

## Decisions

**Queue + single serial worker, not per-call concurrency or contextvar
isolation.** A `PipelineService` wraps a `Pipeline` with an internal
`asyncio.Queue` of `(row, Future)` pairs and one long-lived worker
coroutine that pops one pair at a time, calls `pipeline.process(RawRecord(
value=row, time=...))`, then resolves the `Future` with whatever the
collector gathered. `process_one` becomes: create a `Future`, `put` it on
the queue, `await` it.

Two earlier options were considered and rejected:
- *Per-call `add_sink`/`remove_sink` around each row* — rejected because
  both mutate `Pipeline._sinks`/`_steps` and set `_dirty = True`, forcing a
  chain rebuild; concurrent calls doing this racily would corrupt shared
  pipeline state.
- *True pipeline-level concurrency with `contextvars`-scoped result
  collection* — technically sound for isolating *results* (each `asyncio`
  Task copies its context on creation, so concurrent `process_one` calls in
  separate tasks would never see each other's collected list) but doesn't
  solve the deeper problem: existing `Handler`s hold shared mutable
  instance state with no concurrency protection. Allowing two rows into the
  step chain at once reopens races (read-modify-write across an `await`)
  that the whole codebase was written assuming can't happen. Rejected in
  favor of serializing execution and keeping only *submission* non-blocking.

**Collector is a plain list, not a `contextvars.ContextVar`.** A direct
consequence of serializing execution: since only one row is ever mid-flight
through the pipeline, the worker can reset a single mutable list before
each `pipeline.process()` call and read it back after — no isolation
mechanism needed. A `CorrelatedCollector(Sink)` `Handler` (following the
existing `Sink` marker-class convention in `pipeloom.dataio.sinks`) with
two `@step`s (`Record`, `ErrorMessage`) appends into that list; both steps'
`consume` flag is irrelevant, since `Pipeline.process`'s sink-dispatch
branch treats every sink match as terminal regardless of the flag
(confirmed by inspection of `core.py`'s `process()`).

**Row-scoped `PipelineStopped`/`PipelineFailed`, diverging from `run()`.**
The worker wraps each `pipeline.process()` call in its own
`try/except (PipelineStopped, PipelineFailed)`, resolving that row's
`Future` with the exception instead of letting it propagate out of the
worker loop. Both exception types are treated the same way in this mode —
`PipelineStopped`'s original meaning ("stop pulling more rows from the
source") has no natural equivalent in a persistent service, so rather than
give it a different, service-specific meaning, it is normalized to "this
row failed" like `PipelineFailed`. This lets the same `Handler` code be
reused under `run()` or `PipelineService` without behaving unpredictably
differently depending on which one raises it. `run()`'s own handling of
these exceptions is untouched.

**Result ordering is stack/DFS order, inherited as-is from `Pipeline.process`.**
`process()` drives messages off a LIFO stack (with a `reversed()` push for
a single step's list return, so that step's own list still pops in
original order). The collector's list reflects invocation order exactly as
produced by that traversal. No new ordering guarantee is introduced or
promised beyond what `pipeline-core` already produces; this is simply
documented rather than changed.

## Risks / Trade-offs

- **[Risk] No pipeline-level parallelism caps throughput** for pipelines
  dominated by slow I/O-bound steps (e.g. `AgentHandler`'s A2A calls) — one
  slow row delays every row queued behind it. → **Mitigation**: out of
  scope for this change; if needed later, it's an opt-in escalation (e.g.
  running several independent `PipelineService`+`Pipeline` instances behind
  a caller-side load balancer) rather than changing this design's
  single-worker guarantee, which is what keeps existing `Handler`s safe.
- **[Risk] An unbounded queue could grow without limit** if submissions
  arrive faster than the worker can process them. → **Mitigation**: leave
  the queue's `maxsize` a constructor parameter with a sensible bound;
  callers wanting backpressure can size it, or block on `queue.put` with a
  chosen `maxsize` (`asyncio.Queue` already provides this for free).
- **[Trade-off] A single stalled/blocked step wedges every subsequent row**
  until it resolves. This is the direct cost of choosing "non-blocking
  front door, serial execution" over true concurrency — accepted per
  proposal.md's scope, since it directly preserves `Handler` safety.
