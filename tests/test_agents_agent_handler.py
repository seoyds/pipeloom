import pytest
from a2a.types import TaskState
from pydantic import BaseModel

from pipeloom.agents.agent_handler import AgentHandler
from pipeloom.agents.exceptions import AgentCallFailed
from pipeloom.agents.retry import RetryPolicy
from pipeloom.events import RawRecord
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
async def test_call_raises_agent_call_failed_when_stream_never_reaches_terminal_state():
    client = FakeClient([[task_event(TaskState.TASK_STATE_WORKING)]])
    handler = AgentHandler(
        agent_url="https://example.invalid/agent", client=client, retry_policy=_fast_retry_policy(1)
    )

    with pytest.raises(AgentCallFailed) as exc_info:
        await handler.call(RawRecord(value={}, time=0.0))

    assert exc_info.value.reason == "no_terminal_state"
