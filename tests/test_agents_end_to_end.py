import pytest
from a2a.types import TaskState

from pipeloom.agents.agent_handler import AgentHandler
from pipeloom.agents.retry import RetryPolicy
from pipeloom.core import Handler, Pipeline, step
from pipeloom.dataio.sinks import InMemorySink
from pipeloom.dataio.sources import IterableSource
from pipeloom.events import RawRecord
from pipeloom.pipeline import run
from pipeloom.stages import Stage
from tests.agent_test_helpers import FakeClient, task_event, text_artifact


class UppercaseKeys(Handler):
    """A local (non-agent) VALIDATE-stage step feeding into the agent step,
    proving local and agent-backed Handlers compose in one Pipeline."""

    @step(Stage.VALIDATE, consume=True)
    def go(self, message: RawRecord) -> RawRecord:
        return RawRecord(
            value={k.upper(): v for k, v in message.value.items()}, time=message.time
        )


@pytest.mark.asyncio
async def test_pipeline_routes_through_a_local_handler_then_an_agent_handler_to_the_sink():
    client = FakeClient([[
        task_event(TaskState.TASK_STATE_COMPLETED, artifacts=[text_artifact('{"category": "greeting"}')])
    ]])
    agent = AgentHandler(agent_url="https://example.invalid/agent", client=client)

    pipeline = Pipeline()
    sink = InMemorySink()
    pipeline.add_handler(UppercaseKeys())
    pipeline.add_handler(agent)
    pipeline.add_handler(sink)

    source = IterableSource([{"text": "hi"}])
    await run(source, pipeline)

    assert sink.records == [{"category": "greeting"}]
    assert sink.errors == []


@pytest.mark.asyncio
async def test_pipeline_routes_an_agent_failure_to_the_error_sink_and_continues():
    client = FakeClient([[task_event(TaskState.TASK_STATE_INPUT_REQUIRED)]])
    agent = AgentHandler(
        agent_url="https://example.invalid/agent",
        client=client,
        retry_policy=RetryPolicy(max_attempts=1, backoff_base=0.0, jitter=False),
    )

    pipeline = Pipeline()
    sink = InMemorySink()
    pipeline.add_handler(agent)
    pipeline.add_handler(sink)

    source = IterableSource([{"text": "hi"}])
    await run(source, pipeline)

    assert sink.records == []
    assert len(sink.errors) == 1
    assert sink.errors[0].exception.reason == "input_required"
