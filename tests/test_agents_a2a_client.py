import pytest
from a2a.types import (
    Artifact,
    Part,
    StreamResponse,
    Task,
    TaskArtifactUpdateEvent,
    TaskState,
    TaskStatus,
    TaskStatusUpdateEvent,
)

from pipeloom.agents.a2a_client import (
    build_request,
    collect_terminal_task,
    extract_output,
    is_terminal_state,
)
from pipeloom.agents.exceptions import AttemptFailed
from tests.agent_test_helpers import FakeClient, task_event, text_artifact


def test_is_terminal_state_true_for_real_terminal_states():
    for state in (
        TaskState.TASK_STATE_COMPLETED,
        TaskState.TASK_STATE_FAILED,
        TaskState.TASK_STATE_REJECTED,
        TaskState.TASK_STATE_CANCELED,
        TaskState.TASK_STATE_INPUT_REQUIRED,
        TaskState.TASK_STATE_AUTH_REQUIRED,
    ):
        assert is_terminal_state(state)


def test_is_terminal_state_false_for_in_progress_and_unspecified():
    for state in (
        TaskState.TASK_STATE_UNSPECIFIED,
        TaskState.TASK_STATE_SUBMITTED,
        TaskState.TASK_STATE_WORKING,
    ):
        assert not is_terminal_state(state)


def test_build_request_wraps_payload_as_json_text_part():
    request = build_request({"a": 1})
    assert request.message.parts[0].text == '{"a": 1}'
    assert request.message.role == 1  # Role.ROLE_USER


@pytest.mark.asyncio
async def test_collect_terminal_task_returns_task_from_single_event():
    client = FakeClient([[task_event(TaskState.TASK_STATE_COMPLETED)]])
    task = await collect_terminal_task(client, build_request({}))
    assert task.status.state == TaskState.TASK_STATE_COMPLETED


@pytest.mark.asyncio
async def test_collect_terminal_task_accumulates_status_updates_across_events():
    events = [
        task_event(TaskState.TASK_STATE_SUBMITTED),
        StreamResponse(status_update=TaskStatusUpdateEvent(
            task_id="task-1", status=TaskStatus(state=TaskState.TASK_STATE_WORKING)
        )),
        StreamResponse(status_update=TaskStatusUpdateEvent(
            task_id="task-1", status=TaskStatus(state=TaskState.TASK_STATE_COMPLETED)
        )),
    ]
    client = FakeClient([events])
    task = await collect_terminal_task(client, build_request({}))
    assert task.status.state == TaskState.TASK_STATE_COMPLETED


@pytest.mark.asyncio
async def test_collect_terminal_task_accumulates_artifact_updates():
    events = [
        task_event(TaskState.TASK_STATE_SUBMITTED),
        StreamResponse(artifact_update=TaskArtifactUpdateEvent(
            task_id="task-1", artifact=text_artifact('{"x": 1}')
        )),
        StreamResponse(status_update=TaskStatusUpdateEvent(
            task_id="task-1", status=TaskStatus(state=TaskState.TASK_STATE_COMPLETED)
        )),
    ]
    client = FakeClient([events])
    task = await collect_terminal_task(client, build_request({}))
    assert len(task.artifacts) == 1
    assert task.artifacts[0].parts[0].text == '{"x": 1}'


@pytest.mark.asyncio
async def test_each_call_replays_the_scripted_stream_unmutated():
    # collect_terminal_task accumulates by mutating the Task from the first
    # "task" event in place. In production every attempt gets a freshly
    # deserialized stream, so FakeClient must hand out copies too -- otherwise a
    # retried multi-event stream starts attempt 2 already terminal and silently
    # skips the accumulation it claims to exercise.
    events = [
        task_event(TaskState.TASK_STATE_SUBMITTED),
        StreamResponse(artifact_update=TaskArtifactUpdateEvent(
            task_id="task-1", artifact=text_artifact('{"x": 1}')
        )),
        StreamResponse(status_update=TaskStatusUpdateEvent(
            task_id="task-1", status=TaskStatus(state=TaskState.TASK_STATE_COMPLETED)
        )),
    ]
    client = FakeClient([events])

    first = await collect_terminal_task(client, build_request({}))
    second = await collect_terminal_task(client, build_request({}))

    assert client.calls == 2
    assert first is not second
    for task in (first, second):
        assert task.status.state == TaskState.TASK_STATE_COMPLETED
        assert len(task.artifacts) == 1
        assert task.artifacts[0].parts[0].text == '{"x": 1}'
    # The script itself is untouched, so a third attempt would still start from
    # SUBMITTED with no artifacts.
    assert events[0].task.status.state == TaskState.TASK_STATE_SUBMITTED
    assert len(events[0].task.artifacts) == 0


@pytest.mark.asyncio
async def test_collect_terminal_task_raises_attempt_failed_when_stream_ends_without_terminal_state():
    events = [task_event(TaskState.TASK_STATE_WORKING)]
    client = FakeClient([events])
    with pytest.raises(AttemptFailed) as exc_info:
        await collect_terminal_task(client, build_request({}))
    assert exc_info.value.reason == "no_terminal_state"


def test_extract_output_parses_json_from_text_part():
    task = Task(
        id="t1",
        status=TaskStatus(state=TaskState.TASK_STATE_COMPLETED),
        artifacts=[text_artifact('{"a": 1, "b": [1, 2]}')],
    )
    assert extract_output(task) == {"a": 1, "b": [1, 2]}


def test_extract_output_reads_structured_data_part():
    from google.protobuf import json_format
    from google.protobuf.struct_pb2 import Value

    value = Value()
    json_format.ParseDict({"a": 1}, value)
    task = Task(
        id="t1",
        status=TaskStatus(state=TaskState.TASK_STATE_COMPLETED),
        artifacts=[Artifact(artifact_id="a1", parts=[Part(data=value)])],
    )
    assert extract_output(task) == {"a": 1.0}


def test_extract_output_raises_attempt_failed_on_invalid_json_text():
    task = Task(
        id="t1",
        status=TaskStatus(state=TaskState.TASK_STATE_COMPLETED),
        artifacts=[text_artifact("not json")],
    )
    with pytest.raises(AttemptFailed) as exc_info:
        extract_output(task)
    assert exc_info.value.reason == "invalid_output"


def test_extract_output_raises_attempt_failed_on_no_artifacts():
    task = Task(id="t1", status=TaskStatus(state=TaskState.TASK_STATE_COMPLETED), artifacts=[])
    with pytest.raises(AttemptFailed) as exc_info:
        extract_output(task)
    assert exc_info.value.reason == "invalid_output"
