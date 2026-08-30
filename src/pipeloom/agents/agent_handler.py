import asyncio
from typing import Optional, Type

from a2a.client import Client, create_client
from pydantic import BaseModel, ValidationError

from pipeloom.core import Handler, step
from pipeloom.events import Record, RawRecord
from pipeloom.exceptions import PipelineFailed, PipelineStopped
from pipeloom.stages import Stage

from pipeloom.agents.a2a_client import (
    NON_RETRYABLE_TASK_STATES,
    RETRYABLE_TASK_STATES,
    STATE_REASONS,
    build_request,
    collect_terminal_task,
    extract_output,
)
from pipeloom.agents.exceptions import AgentCallFailed, AttemptFailed
from pipeloom.agents.retry import RetryPolicy, compute_backoff_delay


class AgentHandler(Handler):
    """A Handler backed by a remote AI agent reached over A2A.

    Sends message.value (a plain dict) as the agent's input, retries
    transient failures per retry_policy, and validates the completed task's
    output against output_schema (if provided) before emitting a Record.
    Any terminal failure — retryable-and-exhausted, or immediately for a
    non-retryable task state — raises AgentCallFailed; Pipeline.process's
    existing exception-to-ErrorMessage conversion handles the rest.
    """

    def __init__(
        self,
        agent_url: str,
        retry_policy: Optional[RetryPolicy] = None,
        output_schema: Optional[Type[BaseModel]] = None,
        client: Optional[Client] = None,
    ) -> None:
        self._agent_url = agent_url
        self._retry_policy = retry_policy or RetryPolicy()
        self._output_schema = output_schema
        self._client = client

    async def _get_client(self) -> Client:
        if self._client is None:
            self._client = await create_client(agent=self._agent_url)
        return self._client

    async def _attempt(self, payload: dict) -> dict:
        client = await self._get_client()
        request = build_request(payload)
        task = await collect_terminal_task(client, request)
        state = task.status.state

        if state in NON_RETRYABLE_TASK_STATES:
            raise AgentCallFailed(
                reason=STATE_REASONS[state],
                message=f"Agent task requires external input (state={state})",
            )
        if state in RETRYABLE_TASK_STATES:
            raise AttemptFailed(reason=STATE_REASONS[state])

        result = extract_output(task)
        if self._output_schema is not None:
            try:
                validated = self._output_schema.model_validate(result)
            except ValidationError as exc:
                raise AttemptFailed(reason="invalid_output") from exc
            return validated.model_dump()
        return result

    async def _invoke(self, payload: dict) -> dict:
        attempt = 0
        reason = "timeout"
        while True:
            attempt += 1
            try:
                return await self._attempt(payload)
            except AgentCallFailed:
                raise
            except AttemptFailed as exc:
                reason = exc.reason
            except (PipelineStopped, PipelineFailed, asyncio.CancelledError):
                raise
            except Exception:
                reason = "timeout"
            if attempt >= self._retry_policy.max_attempts:
                raise AgentCallFailed(
                    reason=reason,
                    message=f"Agent call did not succeed after {attempt} attempt(s); last reason: {reason}",
                )
            await asyncio.sleep(compute_backoff_delay(self._retry_policy, attempt))

    @step(Stage.AGENT, consume=True)
    async def call(self, message: RawRecord) -> Record:
        payload = await self._invoke(message.value)
        return Record(payload)
