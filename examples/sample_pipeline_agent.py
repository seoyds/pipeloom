"""A pipeloom pipeline with a step backed by a real AI agent over A2A
(pipeloom.agents.AgentHandler, `agents` extra) -- talking to a real Claude
model, not a test double.

Starts llm_agent_server.py (a real A2A server whose skill classifies a
customer support message with Claude, `agent-example` extra:
a2a-sdk[http-server], anthropic, uvicorn) in-process, then drives two rows
through a pipeline whose AGENT-stage step is AgentHandler pointed at that
server over real HTTP -- exactly like pointing it at any other running A2A
agent, since AgentHandler has no idea the server happens to live in the
same process. One row gets a real classification back from Claude; the
other is missing the required field and gets routed to input-required
instead of guessing.

Needs Claude API credentials: set ANTHROPIC_API_KEY, or run `ant auth
login` first. Install both extras first: `uv sync --extra agents --extra
agent-example`.

To run the agent as a genuinely separate, long-lived service instead of
in-process, start `python examples/llm_agent_server.py` in one terminal and
point AgentHandler(agent_url=...) at it from another process -- this script
only runs it in-process so the whole demo is a single command.
"""

import asyncio
import logging
from typing import Tuple

import uvicorn
from llm_agent_server import build_app

from pipeloom.agents.agent_handler import AgentHandler
from pipeloom.agents.retry import RetryPolicy
from pipeloom.core import Handler, Pipeline, step
from pipeloom.dataio.sinks import InMemorySink  # noqa: F401 -- registers sink_type="memory"
from pipeloom.dataio.sources import IterableSource
from pipeloom.messages import ErrorMessage, RawRecord, Record
from pipeloom.pipeline import run
from pipeloom.stages import Stage

logger = logging.getLogger(__name__)

HOST = "127.0.0.1"
PORT = 9987


class DropUnknownFields(Handler):
    """Local (non-agent) step feeding into the agent step -- local and
    agent-backed Handlers compose in one Pipeline just like any other."""

    @step(Stage.VALIDATE, consume=True)
    def go(self, message: RawRecord) -> RawRecord:
        return RawRecord(value={"text": message.value.get("text", "")}, time=message.time)


async def _start_server() -> Tuple[uvicorn.Server, "asyncio.Task[None]"]:
    app = build_app(url=f"http://{HOST}:{PORT}")
    config = uvicorn.Config(app, host=HOST, port=PORT, log_level="warning")
    server = uvicorn.Server(config)
    task = asyncio.create_task(server.serve())
    while not server.started:
        await asyncio.sleep(0.05)
    return server, task


async def main() -> None:
    server, server_task = await _start_server()
    try:
        agent = AgentHandler(
            agent_url=f"http://{HOST}:{PORT}",
            retry_policy=RetryPolicy(max_attempts=2, backoff_base=0.5, jitter=False),
        )

        pipeline = Pipeline()
        pipeline.add_handler(DropUnknownFields())
        pipeline.add_handler(agent)
        records_sink = pipeline.add_sink(sink_type="memory", sink_trigger=Record)
        errors_sink = pipeline.add_sink(sink_type="memory", sink_trigger=ErrorMessage)

        source = IterableSource(
            [
                {"text": "I was charged twice for my subscription this month, please refund the extra charge."},
                {"text": ""},  # missing -- the agent asks for more input instead of guessing
            ]
        )
        await run(source, pipeline)

        logger.info("Records (from Claude): %s", records_sink.received)
        logger.info("Errors: %s", [(e.reason, str(e.exception)) for e in errors_sink.received])
        logger.info("Agent metrics: %s", agent.metrics)
    finally:
        server.should_exit = True
        await server_task


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-5s %(name)s: %(message)s")
    asyncio.run(main())
