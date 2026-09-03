## 1. Module scaffolding

- [x] 1.1 Create `src/pipeloom/service.py` with no changes to any existing
      `pipeloom` module (`core`, `pipeline`, `messages`, `dataio/*`).
- [x] 1.2 Add `tests/test_service.py` following this repo's existing test
      file naming (`test_pipeline.py`, `test_sinks.py`, etc.).

## 2. Result collector

- [x] 2.1 Write a failing test: a `CorrelatedCollector`-style `Sink`
      `Handler` with `Record`/`ErrorMessage` steps appends matching
      messages, in order, into a list it's given.
- [x] 2.2 Implement the collector to make it pass.
- [x] 2.3 Test: registering the collector via `add_sink` does not prevent
      another independently registered sink (e.g. `InMemorySink`) from also
      receiving the same messages (coexistence, per spec).

## 3. Queue + serial worker

- [x] 3.1 Write a failing test: constructing `PipelineService(pipeline)`
      and awaiting `process_one({...})` for a pipeline that emits one
      `Record` returns `[Record(...)]`.
- [x] 3.2 Implement `PipelineService`: internal `asyncio.Queue` of
      `(RawRecord, asyncio.Future)`, one worker coroutine started
      lazily/on first use, `process_one` enqueues and awaits its `Future`.
- [x] 3.3 Test: a row producing zero terminal messages resolves with `[]`.
- [x] 3.4 Test: a row producing multiple terminal messages resolves with
      all of them, in emission order.
- [x] 3.5 Test (serial execution): using a `Handler` step that records
      entry/exit timestamps (or an explicit ordering marker) around an
      `await asyncio.sleep(...)`, submit two rows concurrently via
      `asyncio.gather(service.process_one(a), service.process_one(b))` and
      assert the second row's step does not start until the first row's
      step has finished — proving no pipeline-level overlap.
- [x] 3.6 Test (non-blocking submission): assert that calling
      `process_one` for a second row does not itself await until the first
      row's processing completes — i.e. both calls can be initiated
      (queued) before either resolves.

## 4. Row-scoped fatal errors

- [x] 4.1 Write a failing test: a `Handler` step that raises
      `PipelineFailed` for one specific row causes that row's
      `process_one` call to raise `PipelineFailed`, while a second,
      unrelated row submitted afterward still processes normally and
      returns its own result.
- [x] 4.2 Write the equivalent test for `PipelineStopped` (same row-scoped,
      non-halting behavior).
- [x] 4.3 Implement: wrap each `pipeline.process()` call in the worker loop
      with `try/except (PipelineStopped, PipelineFailed)`, resolving that
      row's `Future` with the exception via `future.set_exception(exc)`
      instead of letting it propagate out of the worker loop.
- [x] 4.4 Test: confirm `run()`'s existing behavior for the same exceptions
      is unchanged (regression check against `pipeloom.pipeline.run`, not
      new behavior — should already pass without modification).

## 5. Documentation

- [x] 5.1 Add a "Service (request/response)" section to `README.md`
      describing `PipelineService`/`process_one`, its non-blocking-front-
      door/serial-execution model, and how it differs from `run()`
      (per-row fatal errors don't halt the service; no shutdown needed).

## 6. Verification

- [x] 6.1 Run `uv run pytest -q` — full suite passes, including new
      `test_service.py`.
- [x] 6.2 Manually exercise a small example pipeline through
      `PipelineService` (e.g. extend or reference `examples/sample_pipeline.py`
      style usage) to confirm the documented behavior end-to-end.
