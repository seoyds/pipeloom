import pytest
from a2a.types import TaskState
from pydantic import BaseModel

from pipeloom.agents.agent_handler import AgentHandler
from pipeloom.agents.exceptions import AgentCallFailed
from pipeloom.agents.retry import RetryPolicy
from pipeloom.messages import RawRecord
from tests.agent_test_helpers import FakeClient, task_event, text_artifact


def _fast_retry_policy(max_attempts: int) -> RetryPolicy:
    # backoff_base=0.0 keeps asyncio.sleep delays at 0 seconds so tests run instantly.
    return RetryPolicy(max_attempts=max_attempts, backoff_base=0.0, jitter=False)


class ScoreSchema(BaseModel):
    score: int


@pytest.mark.asyncio
async def test_call_returns_record_from_completed_task_output():
    client = FakeClient([[
        task_event(TaskState.TASK_STATE_COMPLETED, artifacts=[text_artifact('{"score": 42}')])
    ]])
    handler = AgentHandler(agent_url="https://example.invalid/agent", client=client)

    result = await handler.call(RawRecord(value={"text": "hello"}, time=0.0))

    assert dict(result) == {"score": 42}
    assert client.calls == 1


@pytest.mark.asyncio
async def test_call_validates_output_against_schema_when_provided():
    client = FakeClient([[
        task_event(TaskState.TASK_STATE_COMPLETED, artifacts=[text_artifact('{"score": 42}')])
    ]])
    handler = AgentHandler(
        agent_url="https://example.invalid/agent", client=client, output_schema=ScoreSchema
    )

    result = await handler.call(RawRecord(value={}, time=0.0))

    assert dict(result) == {"score": 42}


@pytest.mark.asyncio
async def test_call_retries_on_retryable_task_state_then_succeeds():
    client = FakeClient([
        [task_event(TaskState.TASK_STATE_FAILED)],
        [task_event(TaskState.TASK_STATE_COMPLETED, artifacts=[text_artifact('{"ok": true}')])],
    ])
    handler = AgentHandler(
        agent_url="https://example.invalid/agent", client=client, retry_policy=_fast_retry_policy(3)
    )

    result = await handler.call(RawRecord(value={}, time=0.0))

    assert dict(result) == {"ok": True}
    assert client.calls == 2


@pytest.mark.asyncio
async def test_call_raises_agent_call_failed_after_exhausting_retries_on_retryable_state():
    client = FakeClient([[task_event(TaskState.TASK_STATE_FAILED)]])
    handler = AgentHandler(
        agent_url="https://example.invalid/agent", client=client, retry_policy=_fast_retry_policy(2)
    )

    with pytest.raises(AgentCallFailed) as exc_info:
        await handler.call(RawRecord(value={}, time=0.0))

    assert exc_info.value.reason == "failed"
    assert client.calls == 2


@pytest.mark.asyncio
async def test_call_raises_immediately_on_non_retryable_state_without_retrying():
    client = FakeClient([[task_event(TaskState.TASK_STATE_INPUT_REQUIRED)]])
    handler = AgentHandler(
        agent_url="https://example.invalid/agent", client=client, retry_policy=_fast_retry_policy(5)
    )

    with pytest.raises(AgentCallFailed) as exc_info:
        await handler.call(RawRecord(value={}, time=0.0))

    assert exc_info.value.reason == "input_required"
    assert client.calls == 1


@pytest.mark.asyncio
async def test_call_retries_on_invalid_output_then_raises_after_exhausting_attempts():
    client = FakeClient([[
        task_event(TaskState.TASK_STATE_COMPLETED, artifacts=[text_artifact("not json")])
    ]])
    handler = AgentHandler(
        agent_url="https://example.invalid/agent", client=client, retry_policy=_fast_retry_policy(2)
    )

    with pytest.raises(AgentCallFailed) as exc_info:
        await handler.call(RawRecord(value={}, time=0.0))

    assert exc_info.value.reason == "invalid_output"
    assert client.calls == 2


@pytest.mark.asyncio
async def test_call_retries_on_raw_exception_then_raises_timeout_reason():
    client = FakeClient([TimeoutError("connection reset"), TimeoutError("connection reset")])
    handler = AgentHandler(
        agent_url="https://example.invalid/agent", client=client, retry_policy=_fast_retry_policy(2)
    )

    with pytest.raises(AgentCallFailed) as exc_info:
        await handler.call(RawRecord(value={}, time=0.0))

    assert exc_info.value.reason == "timeout"
    assert client.calls == 2


@pytest.mark.asyncio
async def test_call_labels_an_unexpected_exception_internal_error_rather_than_timeout():
    # A RuntimeError is neither OSError nor asyncio.TimeoutError -- it is a bug,
    # not a transport failure, and must not be reported to the operator as one.
    client = FakeClient([RuntimeError("sdk shape changed"), RuntimeError("sdk shape changed")])
    handler = AgentHandler(
        agent_url="https://example.invalid/agent", client=client, retry_policy=_fast_retry_policy(2)
    )

    with pytest.raises(AgentCallFailed) as exc_info:
        await handler.call(RawRecord(value={}, time=0.0))

    assert exc_info.value.reason == "internal_error"
    assert client.calls == 2


@pytest.mark.asyncio
async def test_agent_call_failed_chains_the_exception_that_caused_it():
    client = FakeClient([RuntimeError("sdk shape changed")])
    handler = AgentHandler(
        agent_url="https://example.invalid/agent", client=client, retry_policy=_fast_retry_policy(1)
    )

    with pytest.raises(AgentCallFailed) as exc_info:
        await handler.call(RawRecord(value={}, time=0.0))

    cause = exc_info.value.__cause__
    assert isinstance(cause, RuntimeError)
    assert str(cause) == "sdk shape changed"


@pytest.mark.asyncio
async def test_call_raises_agent_call_failed_when_stream_never_reaches_terminal_state():
    client = FakeClient([[task_event(TaskState.TASK_STATE_WORKING)]])
    handler = AgentHandler(
        agent_url="https://example.invalid/agent", client=client, retry_policy=_fast_retry_policy(1)
    )

    with pytest.raises(AgentCallFailed) as exc_info:
        await handler.call(RawRecord(value={}, time=0.0))

    assert exc_info.value.reason == "no_terminal_state"


@pytest.mark.asyncio
async def test_metrics_track_calls_attempts_and_outcomes_on_success():
    client = FakeClient([
        [task_event(TaskState.TASK_STATE_FAILED)],
        [task_event(TaskState.TASK_STATE_COMPLETED, artifacts=[text_artifact('{"ok": true}')])],
    ])
    handler = AgentHandler(
        agent_url="https://example.invalid/agent", client=client, retry_policy=_fast_retry_policy(3)
    )

    await handler.call(RawRecord(value={}, time=0.0))

    assert handler.metrics.calls == 1
    assert handler.metrics.total_attempts == 2
    assert handler.metrics.successes == 1
    assert handler.metrics.failures == 0


@pytest.mark.asyncio
async def test_metrics_track_failures_when_retries_are_exhausted():
    client = FakeClient([[task_event(TaskState.TASK_STATE_FAILED)]])
    handler = AgentHandler(
        agent_url="https://example.invalid/agent", client=client, retry_policy=_fast_retry_policy(2)
    )

    with pytest.raises(AgentCallFailed):
        await handler.call(RawRecord(value={}, time=0.0))

    assert handler.metrics.calls == 1
    assert handler.metrics.total_attempts == 2
    assert handler.metrics.successes == 0
    assert handler.metrics.failures == 1


@pytest.mark.asyncio
async def test_metrics_reads_usage_from_task_metadata_when_present():
    from google.protobuf import json_format
    from google.protobuf.struct_pb2 import Struct
    from a2a.types import StreamResponse, Task, TaskStatus

    usage_struct = Struct()
    json_format.ParseDict({"usage": {"input_tokens": 12, "output_tokens": 5}}, usage_struct)
    task = Task(
        id="t1",
        status=TaskStatus(state=TaskState.TASK_STATE_COMPLETED),
        artifacts=[text_artifact('{"ok": true}')],
        metadata=usage_struct,
    )
    client = FakeClient([[StreamResponse(task=task)]])
    handler = AgentHandler(agent_url="https://example.invalid/agent", client=client)

    await handler.call(RawRecord(value={}, time=0.0))

    assert handler.metrics.last_usage == {"input_tokens": 12, "output_tokens": 5}


@pytest.mark.asyncio
async def test_metrics_last_usage_is_none_when_no_usage_metadata_present():
    client = FakeClient([[task_event(TaskState.TASK_STATE_COMPLETED, artifacts=[text_artifact('{"ok": true}')])]])
    handler = AgentHandler(agent_url="https://example.invalid/agent", client=client)

    await handler.call(RawRecord(value={}, time=0.0))

    assert handler.metrics.last_usage is None
