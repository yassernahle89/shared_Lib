import json
import os
import logging
from confluent_kafka import Producer, KafkaException

logger = logging.getLogger(__name__)


class KafkaProducerService:
    """
    Thin wrapper around confluent_kafka.Producer for producing JSON messages.
    """

    def __init__(self, bootstrap_servers: str, flush_timeout: float = None, **producer_config):
        config = {"bootstrap.servers": bootstrap_servers}
        config.update(producer_config)
        self._producer = Producer(config)
        self._flush_timeout = (
            flush_timeout
            if flush_timeout is not None
            else float(os.environ.get("PRODUCER_FLUSH_TIMEOUT_SECONDS", 10))
        )

    def produce(self, topic: str, value: dict, key: str = None, flush: bool = True) -> None:
        """
        Serialize `value` as JSON and produce it to `topic`.

        Raises on serialization, queueing, or delivery errors so callers can
        decide how to handle failures (e.g. re-raise to avoid committing
        the triggering consumer offset). When flush=True (default), this
        call blocks until the broker has acknowledged the message (or the
        flush timeout is hit) and raises if delivery did not succeed.
        """
        if not topic:
            raise ValueError("produce() called with empty topic")

        try:
            payload = json.dumps(value).encode("utf-8")
        except (TypeError, ValueError) as e:
            logger.error(f"Failed to serialize message for topic={topic}: {e}")
            raise

        # Mutable container so the delivery callback (invoked later, during
        # poll()/flush()) can report the outcome of THIS specific produce()
        # call back to us.
        delivery_result = {"done": False, "err": None}

        def _on_delivery(err, msg):
            delivery_result["done"] = True
            delivery_result["err"] = err
            if err is not None:
                logger.error(f"Message delivery failed for topic={topic}: {err}")
            else:
                logger.info(
                    f"Message delivered to {msg.topic()} [partition {msg.partition()}] "
                    f"at offset {msg.offset()}"
                )

        try:
            self._producer.produce(
                topic=topic,
                key=key.encode("utf-8") if key else None,
                value=payload,
                callback=_on_delivery,
            )
            # Serve delivery callbacks (non-blocking check of queue)
            self._producer.poll(0)
        except BufferError as e:
            logger.error(f"Local producer queue is full for topic={topic}: {e}")
            raise
        except Exception as e:
            logger.error(f"Failed to enqueue message for topic={topic}: {e}")
            raise

        if not flush:
            return

        remaining = self._producer.flush(timeout=self._flush_timeout)
        if remaining > 0:
            logger.error(
                f"{remaining} message(s) still in queue after flush timeout "
                f"for topic={topic}"
            )
            raise KafkaException(f"Timed out flushing message to topic={topic}")

        if not delivery_result["done"]:
            # Shouldn't happen if flush() returned 0, but guard anyway.
            logger.error(f"Delivery callback never fired for topic={topic}")
            raise KafkaException(f"No delivery confirmation for topic={topic}")

        if delivery_result["err"] is not None:
            raise KafkaException(
                f"Broker rejected message for topic={topic}: {delivery_result['err']}"
            )

    def close(self):
        try:
            self._producer.flush(timeout=self._flush_timeout)
        except Exception as e:
            logger.error(f"Error flushing producer during close: {e}")