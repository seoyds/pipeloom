## Purpose

Lets a caller submit one row to an existing `Pipeline` and get back exactly
what that pipeline produced for it, so pipeloom can be embedded behind a
synchronous request/response call instead of only driven end-to-end by
`run()` over a `Source`.

## ADDED Requirements

### Requirement: Submit One Row for Processing
The system SHALL provide a way to submit a single row's data to a
`Pipeline` and receive back every terminal message that row produced.

#### Scenario: A row that produces one Record
- **WHEN** a row is submitted whose processing produces exactly one `Record`
- **THEN** the submission resolves with a list containing that one `Record`

#### Scenario: A row that produces no terminal messages
- **WHEN** a row is submitted and its processing produces no `Record` or `ErrorMessage`
- **THEN** the submission resolves with an empty list

#### Scenario: A row that produces multiple terminal messages
- **WHEN** a row is submitted whose processing produces more than one `Record`/`ErrorMessage`
- **THEN** the submission resolves with a list containing all of them, in the order they were produced

#### Scenario: A row that fails becomes an ErrorMessage result
- **WHEN** a step processing the row raises an exception that becomes an `ErrorMessage` (see [[pipeline-core]] error-as-data)
- **THEN** that `ErrorMessage` is included in the submission's result list like any other terminal message

### Requirement: Non-Blocking Submission
Submitting a row SHALL NOT block on, or wait for, any other row's
submission to be accepted.

#### Scenario: Multiple rows can be submitted without waiting on each other
- **WHEN** several rows are submitted in close succession, before earlier ones have finished processing
- **THEN** every submission is accepted immediately rather than being rejected or forced to wait for an earlier row's processing to complete before being accepted

### Requirement: Serial Row Processing
Rows submitted this way SHALL be processed through the pipeline one at a
time; no two rows' processing SHALL overlap within the same pipeline.

#### Scenario: A second row is not started until the first finishes
- **WHEN** two rows are submitted to the same pipeline in quick succession
- **THEN** processing of the second row does not begin until processing of the first row (including any steps it awaits on) has completed

### Requirement: Row-Scoped Fatal Errors
A `PipelineStopped` or `PipelineFailed` raised while processing one
submitted row SHALL fail only that row's submission; it SHALL NOT prevent
later submitted rows from being processed.

#### Scenario: A fatal error on one row surfaces to that row's caller
- **WHEN** processing a submitted row raises `PipelineStopped` or `PipelineFailed`
- **THEN** that row's submission fails with that exception

#### Scenario: Processing continues for subsequent rows after a fatal error
- **WHEN** a previously submitted row failed with `PipelineStopped` or `PipelineFailed`
- **THEN** a row submitted afterward is still processed normally

#### Scenario: This diverges from run()'s behavior
- **WHEN** the same `PipelineStopped`/`PipelineFailed` condition occurs during a `run()`-driven batch instead of a submitted row
- **THEN** `run()`'s existing behavior is unchanged: the exception ends that run (or, for `PipelineStopped`, ends it early-but-successfully) exactly as already specified in [[pipeline-core]]

### Requirement: Coexistence With Other Sinks
Rows processed this way SHALL still be visible to every sink already
registered on the pipeline via `add_sink`, unaffected by also collecting
results for the submitting caller.

#### Scenario: An independently registered sink still receives the row's messages
- **WHEN** a pipeline has its own `add_sink`-registered sink (e.g. persisting `Record`s to a file or external system) in addition to being used for row submission
- **THEN** that sink still receives every matching message for the submitted row, exactly as it would if the same message arrived via `run()`

### Requirement: No Persistent Shutdown Lifecycle
Submitting rows this way SHALL NOT require, or depend on, an end-of-run
lifecycle step; there is no equivalent of `run()`'s `Checkpoint`/`Shutdown`
for this mode.

#### Scenario: Rows can keep being submitted indefinitely
- **WHEN** rows are submitted continuously over an arbitrarily long period, with no fixed end
- **THEN** processing continues to accept and process each one without requiring any explicit end-of-run signal
