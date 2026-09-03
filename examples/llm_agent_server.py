"""A real A2A agent server backed by Claude (the `anthropic` SDK) -- not a
test double. Exposes one skill: classify a short customer support message
by topic and sentiment. Importable (see sample_pipeline_agent.py, which
starts it in-process) or fully standalone:

    ANTHROPIC_API_KEY=... PYTHONPATH=src uv run python examples/llm_agent_server.py

then point any A2A client (including a2a-inspector) at http://127.0.0.1:9999.

Requires the `agent-example` extra (a2a-sdk[http-server], anthropic,
uvicorn): `uv sync --extra agent-example`. Needs Claude API credentials --
set ANTHROPIC_API_KEY, or run `ant auth login` first; anthropic.AsyncAnthropic()
resolves either automatically, same as any other Claude API client.
"""

import json
import logging
import os
from typing import Any, Optional

import anthropic
import uvicorn
from starlette.applications import Starlette

from a2a.helpers import get_message_text, new_data_part, new_task_from_user_message, new_text_message
from a2a.server.agent_execution import AgentExecutor, RequestContext
from a2a.server.events import EventQueue
from a2a.server.request_handlers import DefaultRequestHandler
from a2a.server.routes import create_agent_card_routes, create_jsonrpc_routes
from a2a.server.tasks import InMemoryTaskStore, TaskUpdater
from a2a.types import AgentCapabilities, AgentCard, AgentInterface, AgentSkill

logger = logging.getLogger(__name__)

DEFAULT_MODEL = "claude-opus-5"

SYSTEM_PROMPT = (
    "You triage short customer support messages. Given the customer's message, "
    "respond with ONLY a single JSON object -- no markdown, no code fence, no "
    "commentary -- matching this shape: "
    '{"category": "billing"|"technical"|"account"|"general", '
    '"sentiment": "positive"|"neutral"|"negative", "summary": "<one short sentence>"}.'
)


def _parse_json_response(text: str) -> dict:
    """Claude is instructed to return raw JSON; strip an occasional code
    fence defensively rather than trust the instruction unconditionally."""
    text = text.strip()
    if text.startswith("```"):
        text = text.strip("`")
        if text.lower().startswith("json"):
            text = text[4:]
    return json.loads(text)


class SupportMessageClassifier:
    """Wraps a real call to Claude -- the thing AgentHandler is actually
    talking to over A2A once wired up on the client side."""

    def __init__(self, model: Optional[str] = None) -> None:
        self._client = anthropic.AsyncAnthropic()
        self._model = model or os.environ.get("PIPELOOM_EXAMPLE_MODEL", DEFAULT_MODEL)

    async def classify(self, text: str) -> dict:
        response = await self._client.messages.create(
            model=self._model,
            max_tokens=512,
            system=SYSTEM_PROMPT,
            output_config={"effort": "low"},  # classification is a low-effort task
            messages=[{"role": "user", "content": text}],
        )
        block = next((b for b in response.content if b.type == "text"), None)
        if block is None or not block.text.strip():
            raise ValueError("model returned no text content")
        return _parse_json_response(block.text)


class SupportMessageClassifierExecutor(AgentExecutor):
    """Adapts SupportMessageClassifier to the A2A AgentExecutor interface --
    the same shape as any real agent implementation, following the pattern
    from the a2a-sdk helloworld sample."""

    def __init__(self, model: Optional[str] = None) -> None:
        self._agent = SupportMessageClassifier(model=model)

    async def execute(self, context: RequestContext, event_queue: EventQueue) -> None:
        task = context.current_task
        if task is None:
            task = new_task_from_user_message(context.message)
            await event_queue.enqueue_event(task)

        updater = TaskUpdater(event_queue=event_queue, task_id=task.id, context_id=task.context_id)
        await updater.start_work()

        try:
            payload: Any = json.loads(get_message_text(context.message))
        except json.JSONDecodeError:
            payload = {}
        text = str((payload or {}).get("text") or "").strip()

        if not text:
            # A real reason an agent can't proceed without a human/caller
            # filling in more -- maps to AgentHandler's non-retryable path.
            await updater.requires_input(new_text_message("Missing required field: text"))
            return

        try:
            result = await self._agent.classify(text)
        except Exception as exc:
            logger.exception("classification failed")
            await updater.failed(new_text_message(f"classification failed: {exc}"))
            return

        await updater.add_artifact(parts=[new_data_part(result)], name="classification")
        await updater.complete()

    async def cancel(self, context: RequestContext, event_queue: EventQueue) -> None:
        raise NotImplementedError("Cancel is not supported.")


def build_app(url: str, model: Optional[str] = None) -> Starlette:
    skill = AgentSkill(
        id="classify_support_message",
        name="Classify Support Message",
        description="Categorizes a short customer support message by topic and sentiment.",
        input_modes=["text/plain"],
        output_modes=["application/json"],
        tags=["classification", "support"],
        examples=["My invoice charged me twice this month"],
    )
    agent_card = AgentCard(
        name="pipeloom Example Classifier",
        description="Classifies customer support messages using Claude.",
        version="0.1.0",
        default_input_modes=["text/plain"],
        default_output_modes=["application/json"],
        capabilities=AgentCapabilities(streaming=True),
        supported_interfaces=[AgentInterface(protocol_binding="JSONRPC", url=url, protocol_version="1.0")],
        skills=[skill],
    )
    request_handler = DefaultRequestHandler(
        agent_executor=SupportMessageClassifierExecutor(model=model),
        task_store=InMemoryTaskStore(),
        agent_card=agent_card,
    )
    routes = create_agent_card_routes(agent_card) + create_jsonrpc_routes(request_handler, "/")
    return Starlette(routes=routes)


def main() -> None:
    host = os.environ.get("PIPELOOM_EXAMPLE_HOST", "127.0.0.1")
    port = int(os.environ.get("PIPELOOM_EXAMPLE_PORT", "9999"))
    app = build_app(url=f"http://{host}:{port}")
    uvicorn.run(app, host=host, port=port)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-5s %(name)s: %(message)s")
    main()
