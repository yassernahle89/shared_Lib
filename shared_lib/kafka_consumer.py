from kafka import KafkaConsumer
import threading
import json
import logging

logger = logging.getLogger(__name__)

class KafkaConsumerService:
    def __init__(self, topic, bootstrap_servers="localhost:9092", group_id="default-group", auto_offset_reset="earliest", enable_auto_commit=True, callback=None):
        self.topic = topic
        self.bootstrap_servers = bootstrap_servers
        self.group_id = group_id
        self.auto_offset_reset = auto_offset_reset
        self.enable_auto_commit = enable_auto_commit
        self.callback = callback
        self._thread = None
        self._running = False

    def _consume(self):
        consumer = KafkaConsumer(
            self.topic,
            bootstrap_servers=self.bootstrap_servers,
            group_id=self.group_id,
            auto_offset_reset=self.auto_offset_reset,
            enable_auto_commit=self.enable_auto_commit,
        )

        logger.info(f"Kafka consumer started for topic '{self.topic}'")

        for message in consumer:
            try:
                data = message.value.decode() if isinstance(message.value, bytes) else message.value
                parsed = json.loads(data)
                logger.info(f"Consumed message: {parsed}")
                if self.callback:
                    self.callback(parsed)
            except Exception as e:
                logger.error(f"Error processing message: {e}")

            if not self._running:
                break

        consumer.close()

    def start(self):
        if not self._thread or not self._thread.is_alive():
            self._running = True
            self._thread = threading.Thread(target=self._consume, daemon=True)
            self._thread.start()

    def stop(self):
        self._running = False
        if self._thread:
            self._thread.join()
