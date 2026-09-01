# agents Specification

## Purpose

`AgentHandler` (the `agents` extra) is a [[pipeline-core]] `Handler` backed
by a remote AI agent reached over the A2A protocol: it sends a record's
value as the agent's input, retries transient failures, and validates the
completed task's output before emitting a `Record`.

## Requirements

### Requirement: AgentHandler Step
`AgentHandler.call` SHALL be a `Stage.AGENT`, `consume=True` step that sends
`message.value` to the configured agent and emits a `Record` on success.

#### Scenario: A RawRecord's value becomes the agent request payload
- **WHEN** `AgentHandler` receives a `RawRecord`
- **THEN** `message.value` (a plain dict) is sent as the agent's input over A2A

#### Scenario: A successful call emits a Record
- **WHEN** the agent call completes successfully (with or without an `output_schema`)
- **THEN** `AgentHandler` emits `Record(<output>)` and the input `RawRecord` is consumed

#### Scenario: Any terminal failure raises AgentCallFailed
- **WHEN** the agent call ends without success (see Terminal Task State Classification and Exhausted Retries below)
- **THEN** `AgentCallFailed` is raised, which [[pipeline-core]]'s exception-to-`ErrorMessage` conversion turns into routable data

### Requirement: Terminal Task State Classification
A completed A2A task's terminal state SHALL determine whether the call is
retried or fails immediately.

#### Scenario: A non-retryable state fails immediately
- **WHEN** a task reaches `TASK_STATE_INPUT_REQUIRED` or `TASK_STATE_AUTH_REQUIRED`
- **THEN** `AgentCallFailed` is raised immediately, with no further retry attempts

#### Scenario: A retryable state may be retried
- **WHEN** a task reaches `TASK_STATE_FAILED`, `TASK_STATE_REJECTED`, or `TASK_STATE_CANCELED`
- **THEN** `AttemptFailed` is raised internally, which the retry loop may act on

#### Scenario: A non-terminal stream is treated as retryable
- **WHEN** the A2A event stream ends without ever reporting a real terminal state (only `TASK_STATE_UNSPECIFIED`, `SUBMITTED`, or `WORKING`)
- **THEN** `AttemptFailed(reason="no_terminal_state")` is raised, eligible for retry

### Requirement: Retry Policy
`RetryPolicy(max_attempts=3, backoff_base=0.5, jitter=True)` SHALL govern
how many times a failed attempt is retried and how long to wait between
attempts.

#### Scenario: Backoff grows exponentially per attempt
- **WHEN** computing the delay before retrying after the Nth failed attempt
- **THEN** the base delay is `backoff_base * 2**(N-1)` seconds

#### Scenario: Jitter randomizes the delay
- **WHEN** `jitter` is `True` (the default)
- **THEN** the actual delay is drawn uniformly from `[0, base_delay)` rather than being the exact base delay

#### Scenario: Only certain failures are retried
- **WHEN** an attempt raises `AttemptFailed`, `OSError`, or `asyncio.TimeoutError`, or any other unexpected `Exception`
- **THEN** the failure is recorded and, if attempts remain, retried after the computed backoff delay
- **WHEN** an attempt raises `AgentCallFailed`, `PipelineStopped`, `PipelineFailed`, or `asyncio.CancelledError`
- **THEN** it propagates immediately with no retry

#### Scenario: Retries carry no idempotency key
- **WHEN** an attempt is retried
- **THEN** a fresh A2A request is sent with a new message id and no de-duplication key, so a retry against a non-idempotent agent can duplicate its side effect

### Requirement: Output Schema Validation
When `output_schema` (a Pydantic model) is provided, the extracted task
output SHALL be validated against it before being emitted as a `Record`.

#### Scenario: Valid output is normalized through the schema
- **WHEN** the extracted output validates against `output_schema`
- **THEN** the `Record` is built from the validated model's `model_dump()`, not the raw extracted output

#### Scenario: Invalid output is retryable
- **WHEN** the extracted output fails `output_schema` validation
- **THEN** `AttemptFailed(reason="invalid_output")` is raised, eligible for retry under the retry policy

### Requirement: Output Extraction
`extract_output(task)` SHALL extract a JSON-like payload from a completed
task's first artifact's first part.

#### Scenario: A structured data part is converted directly
- **WHEN** the first part's content is a `data` (protobuf `Value`) part
- **THEN** it is converted to a plain dict/JSON-like value

#### Scenario: A text part is parsed as JSON
- **WHEN** the first part's content is a `text` part
- **THEN** it is parsed as JSON; invalid JSON raises `AttemptFailed(reason="invalid_output")`

#### Scenario: Missing artifacts/parts is invalid output
- **WHEN** the task has no artifacts, or its first artifact has no parts
- **THEN** `AttemptFailed(reason="invalid_output")` is raised

### Requirement: Exhausted Retries
When every retry attempt has been used without success, the call SHALL fail
with the last observed reason.

#### Scenario: The final attempt's reason is reported
- **WHEN** the `max_attempts`-th attempt still fails
- **THEN** `AgentCallFailed` is raised with the last attempt's failure `reason` and the original exception chained as its cause

### Requirement: Call Metrics
`AgentHandler.metrics` (`AgentCallMetrics`) SHALL track call/attempt/outcome
counters and the most recent usage data reported by the agent.

#### Scenario: Calls and attempts are counted independently
- **WHEN** a single `AgentHandler.call` invocation makes 3 attempts before succeeding
- **THEN** `metrics.calls` increments by 1 and `metrics.total_attempts` increments by 3

#### Scenario: Success and failure are tallied
- **WHEN** a call ultimately succeeds
- **THEN** `metrics.successes` increments by 1
- **WHEN** a call ultimately fails (raises `AgentCallFailed`)
- **THEN** `metrics.failures` increments by 1

#### Scenario: Usage metadata is best-effort
- **WHEN** a completed task's metadata includes a `"usage"` key
- **THEN** `metrics.last_usage` is set to that value; when absent, `metrics.last_usage` is `None`
