"""Stateless helpers for talking to an A2A agent via the real a2a-sdk Client.

Kept separate from AgentHandler so the stream-accumulation and
task-classification logic can be tested as plain functions/coroutines
against a fake client, with no retry policy or Handler/@step concerns mixed in.
"""

import json
import uuid
from typing import Any, Optional

from google.protobuf import json_format

from a2a.types import Message as A2AMessage
from a2a.types import Part, Role, SendMessageRequest, Task, TaskState

from pipeloom.agents.exceptions import AttemptFailed

TERMINAL_STATES = {
    TaskState.TASK_STATE_COMPLETED,
    TaskState.TASK_STATE_FAILED,
    TaskState.TASK_STATE_REJECTED,
    TaskState.TASK_STATE_CANCELED,
    TaskState.TASK_STATE_INPUT_REQUIRED,
    TaskState.TASK_STATE_AUTH_REQUIRED,
}

RETRYABLE_TASK_STATES = {
    TaskState.TASK_STATE_FAILED,
    TaskState.TASK_STATE_REJECTED,
    TaskState.TASK_STATE_CANCELED,
}

NON_RETRYABLE_TASK_STATES = {
    TaskState.TASK_STATE_INPUT_REQUIRED,
    TaskState.TASK_STATE_AUTH_REQUIRED,
}

STATE_REASONS = {
    TaskState.TASK_STATE_FAILED: "failed",
    TaskState.TASK_STATE_REJECTED: "rejected",
    TaskState.TASK_STATE_CANCELED: "canceled",
    TaskState.TASK_STATE_INPUT_REQUIRED: "input_required",
    TaskState.TASK_STATE_AUTH_REQUIRED: "auth_required",
}


def is_terminal_state(state) -> bool:
    return state in TERMINAL_STATES


def build_request(payload: dict) -> SendMessageRequest:
    """Wrap a plain dict payload as an A2A user Message, JSON-encoded in a
    text Part."""
    message = A2AMessage(
        message_id=str(uuid.uuid4()),
        role=Role.ROLE_USER,
        parts=[Part(text=json.dumps(payload))],
    )
    return SendMessageRequest(message=message)


async def collect_terminal_task(client, request: SendMessageRequest) -> Task:
    """Iterate client.send_message(request), accumulating a running Task from
    the stream's task/status_update/artifact_update events, until a real
    terminal TaskState is observed.

    TaskStatusUpdateEvent and TaskArtifactUpdateEvent carry only a task_id
    plus a status or artifact — never a full Task — so the running Task is
    built up in place (protobuf messages are mutable) starting from the
    first "task" event seen. "message" events carry no task state and are
    skipped. Raises AttemptFailed(reason="no_terminal_state") if the stream
    ends before any real terminal state is reached (this also covers a
    stream that only ever reports TASK_STATE_UNSPECIFIED, SUBMITTED, or
    WORKING).
    """
    latest_task: Optional[Task] = None
    async for event in client.send_message(request):
        which = event.WhichOneof("payload")
        if which == "task":
            latest_task = event.task
        elif which == "status_update" and latest_task is not None:
            latest_task.status.CopyFrom(event.status_update.status)
        elif which == "artifact_update" and latest_task is not None:
            latest_task.artifacts.append(event.artifact_update.artifact)
        if latest_task is not None and is_terminal_state(latest_task.status.state):
            return latest_task
    raise AttemptFailed(reason="no_terminal_state")


def extract_output(task: Task) -> Any:
    """Extract the JSON-like payload from a completed Task's first artifact's
    first part. `data` parts (structured, google.protobuf.Value) are
    converted directly; `text` parts are parsed as JSON."""
    if not task.artifacts or not task.artifacts[0].parts:
        raise AttemptFailed(reason="invalid_output")
    part = task.artifacts[0].parts[0]
    which = part.WhichOneof("content")
    if which == "data":
        return json_format.MessageToDict(part.data)
    if which == "text":
        try:
            return json.loads(part.text)
        except json.JSONDecodeError as exc:
            raise AttemptFailed(reason="invalid_output") from exc
    raise AttemptFailed(reason="invalid_output")
