import json

import pytest

pytest.importorskip("confluent_kafka")

from pipeloom.core import Pipeline
from pipeloom.dataio.kafka import KafkaSink
from pipeloom.messages import Record


class FakeProducer:
    """Mimics confluent_kafka.Producer's produce/poll/flush surface without a
    live broker -- delivery is synchronous and always succeeds unless
    `fail_next` is set."""

    def __init__(self) -> None:
        self.produced: list = []
        self.flushed = False
        self.fail_next = False

    def produce(self, topic, value=None, key=None, callback=None) -> None:
        self.produced.append({"topic": topic, "value": value, "key": key})
        if callback is not None:
            if self.fail_next:
                self.fail_next = False
                callback(RuntimeError("broker unavailable"), None)
            else:
                callback(None, None)

    def poll(self, timeout: float) -> int:
        return 0

    def flush(self) -> None:
        self.flushed = True


def test_kafka_sink_requires_either_producer_or_config():
    with pytest.raises(ValueError):
        KafkaSink(topic="records")


@pytest.mark.asyncio
async def test_kafka_sink_publishes_records_as_json():
    producer = FakeProducer()
    pipeline = Pipeline()
    sink = pipeline.add_sink(sink_type="kafka", sink_params={"topic": "records", "producer": producer})

    await pipeline.process(Record({"name": "ada"}))
    sink.close()

    assert len(producer.produced) == 1
    assert producer.produced[0]["topic"] == "records"
    assert json.loads(producer.produced[0]["value"]) == {"name": "ada"}
    assert producer.flushed is True


@pytest.mark.asyncio
async def test_kafka_sink_uses_key_fn_to_derive_the_message_key():
    producer = FakeProducer()
    pipeline = Pipeline()
    sink = pipeline.add_sink(
        sink_type="kafka",
        sink_params={"topic": "records", "producer": producer, "key_fn": lambda row: row["name"]},
    )

    await pipeline.process(Record({"name": "ada"}))

    assert producer.produced[0]["key"] == "ada"


def test_kafka_sink_surfaces_a_delivery_failure_as_an_error():
    producer = FakeProducer()
    producer.fail_next = True
    sink = KafkaSink(topic="records", producer=producer)

    with pytest.raises(Exception):
        sink.write(Record({"name": "ada"}))
