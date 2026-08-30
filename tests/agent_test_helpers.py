"""Shared test doubles for pipeloom.agents tests. Not part of the pipeloom
package — imported directly by test modules as `tests.agent_test_helpers`.
"""

from typing import List, Union

from a2a.types import Artifact, Part, StreamResponse, Task, TaskStatus


class FakeClient:
    """Test double for a2a.client.Client — implements only send_message,
    the only Client method AgentHandler calls.

    `responses` is a list where each item is either a list of StreamResponse
    events to yield for that call to send_message, or an exception instance
    to raise instead of yielding anything. If send_message is called more
    times than there are items, the last item is reused for every call
    beyond the list's length.
    """

    def __init__(self, responses: List[Union[List[StreamResponse], BaseException]]) -> None:
        self._responses = responses
        self.calls = 0

    async def send_message(self, request):
        idx = min(self.calls, len(self._responses) - 1)
        self.calls += 1
        item = self._responses[idx]
        if isinstance(item, BaseException):
            raise item
        for event in item:
            yield event


def task_event(state, artifacts=None, task_id: str = "task-1") -> StreamResponse:
    """Build a StreamResponse carrying a full Task in the given state."""
    return StreamResponse(
        task=Task(id=task_id, status=TaskStatus(state=state), artifacts=artifacts or [])
    )


def text_artifact(payload_json: str, artifact_id: str = "artifact-1") -> Artifact:
    """Build an Artifact whose first part is a text Part."""
    return Artifact(artifact_id=artifact_id, parts=[Part(text=payload_json)])
