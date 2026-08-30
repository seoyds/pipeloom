"""Optional Kafka sink -- requires the `kafka` extra (confluent-kafka, a
binding over librdkafka: the fastest of the common Python Kafka clients).

Nothing in pipeloom.dataio.sinks/sources imports this module; import it
explicitly to register sink_type="kafka".
"""

import json
from typing import Any, Callable, Dict, Optional

from confluent_kafka import KafkaException, Producer

from pipeloom.core import register_sink_type
from pipeloom.dataio.sinks import row_from_message
from pipeloom.messages import Message

__all__ = ["KafkaSink"]


class KafkaSink:
    """Publishes rows as JSON to a Kafka topic.

    producer_config is passed straight through to confluent_kafka.Producer
    (must include at least "bootstrap.servers"); pass an already-constructed
    producer instead via `producer` (e.g. a test double). confluent-kafka's
    Producer.produce() only queues the message -- delivery failures surface
    on the next call to write() or close() that serves the producer's
    callbacks, becoming an ErrorMessage like any other step failure. Call
    close() to flush in-flight messages before shutdown.
    """

    def __init__(
        self,
        topic: str,
        producer_config: Optional[Dict[str, Any]] = None,
        producer: Optional[Producer] = None,
        key_fn: Optional[Callable[[dict], Optional[str]]] = None,
    ) -> None:
        if producer is None and producer_config is None:
            raise ValueError("KafkaSink requires either producer_config or producer")
        self._topic = topic
        self._producer = producer if producer is not None else Producer(producer_config)
        self._key_fn = key_fn
        self._delivery_error: Optional[BaseException] = None

    def _on_delivery(self, err, msg) -> None:
        if err is not None:
            self._delivery_error = KafkaException(err)

    def write(self, message: Message) -> None:
        row = row_from_message(message)
        self._producer.produce(
            self._topic,
            value=json.dumps(row).encode("utf-8"),
            key=self._key_fn(row) if self._key_fn else None,
            callback=self._on_delivery,
        )
        self._producer.poll(0)
        if self._delivery_error is not None:
            error, self._delivery_error = self._delivery_error, None
            raise error

    def close(self) -> None:
        self._producer.flush()


register_sink_type("kafka", KafkaSink)
