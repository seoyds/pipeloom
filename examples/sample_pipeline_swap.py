"""Swapping one part of a pipeline without touching the others.

The README's "What a change actually touches" example, runnable. An order
pipeline has three parts, each declaring only what it takes and gives:

    ParseOrder   VALIDATE   RawRecord -> Order
    RuleLabeler  ENRICH     Order     -> Labeled
    Finalize     TRANSFORM  Labeled   -> Record

The rule-based labeller is then replaced by LLMLabeler -- an async step at a
different stage (AGENT) with the same connector, Order -> Labeled. Building
the pipeline is the only place that changes: ParseOrder and Finalize are the
exact same objects in both runs, and never learn anything happened.

LLMLabeler fakes its model call (no network, no API key) so the whole demo is
a single command; see sample_pipeline_agent.py for a step backed by a real
agent over A2A.
"""

import asyncio
import logging
from typing import List

from pipeloom.core import Handler, Pipeline, step
from pipeloom.dataio.sinks import InMemorySink  # noqa: F401 -- registers sink_type="memory"
from pipeloom.dataio.sources import IterableSource
from pipeloom.messages import Message, RawRecord, Record
from pipeloom.pipeline import run
from pipeloom.stages import Stage

logger = logging.getLogger(__name__)

ORDERS = [
    {"sku": "A1", "qty": 500, "note": "restocking the warehouse"},
    {"sku": "B7", "qty": 2, "note": "birthday gift, please wrap"},
]


class Order(Message, dict):
    """A parsed order."""


class Labeled(Message, dict):
    """An order with a category attached -- the connector both labellers share."""


class ParseOrder(Handler):
    @step(Stage.VALIDATE, consume=True)
    def parse(self, message: RawRecord) -> Order:
        """Turn a raw input row into an Order."""
        return Order(message.value)


class RuleLabeler(Handler):
    @step(Stage.ENRICH, consume=True)
    def label(self, message: Order) -> Labeled:
        """Label by a fixed quantity threshold."""
        return Labeled(message, category="bulk" if message["qty"] > 100 else "retail")


class LLMLabeler(Handler):
    @step(Stage.AGENT, consume=True)
    async def label(self, message: Order) -> Labeled:
        """Label with a (faked) model call -- same connector as RuleLabeler."""
        return Labeled(message, category=await self.classify_with_llm(message))

    async def classify_with_llm(self, order: Order) -> str:
        # Stand-in for a real model call: reads the free-text note, which the
        # quantity rule above can't.
        await asyncio.sleep(0)
        return "gift" if "gift" in order["note"] else "bulk" if order["qty"] > 100 else "retail"


class Finalize(Handler):
    @step(Stage.TRANSFORM, consume=True)
    def finish(self, message: Labeled) -> Record:
        """Emit the final output row."""
        return Record(message)


async def run_with(labeler: Handler, parse: ParseOrder, finalize: Finalize) -> List[Record]:
    """Build the pipeline around whichever labeller is plugged in. This is the
    only code that knows which parts exist."""
    pipeline = Pipeline()
    pipeline.add_handler(parse)
    pipeline.add_handler(labeler)  # <- the one line that changes
    pipeline.add_handler(finalize)
    sink = pipeline.add_sink(sink_type="memory", sink_trigger=Record)

    await run(IterableSource(ORDERS), pipeline)
    logger.info(pipeline.format_profile(pipeline.get_profile_data(), blueprint=True))
    return sink.received


async def main() -> None:
    # The same ParseOrder and Finalize instances serve both runs.
    parse, finalize = ParseOrder(), Finalize()

    before = await run_with(RuleLabeler(), parse, finalize)
    after = await run_with(LLMLabeler(), parse, finalize)

    logger.info("With RuleLabeler: %s", [r["category"] for r in before])
    logger.info("With LLMLabeler:  %s", [r["category"] for r in after])


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-5s %(name)s: %(message)s")
    asyncio.run(main())
