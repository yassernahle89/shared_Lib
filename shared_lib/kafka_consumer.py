from confluent_kafka import Consumer
import threading
import json
import logging

logger = logging.getLogger(__name__)

class KafkaConsumerService:
    def __init__(
        self,
        topic,
        bootstrap_servers="localhost:9092",
        group_id="default-group",
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        callback=None,
        extra_config=None,   # optional dict
    ):
        self.topic = topic
        self.bootstrap_servers = bootstrap_servers
        self.group_id = group_id
        self.auto_offset_reset = auto_offset_reset
        self.enable_auto_commit = enable_auto_commit
        self.callback = callback
        self.extra_config = extra_config or {}
        self._thread = None
        self._running = False

    def _build_config(self):
        # base config
        config = {
            "bootstrap.servers": self.bootstrap_servers,
            "group.id": self.group_id,
            "auto.offset.reset": self.auto_offset_reset,
            "enable.auto.commit": self.enable_auto_commit,
        }

        # add your defaults here
        defaults = {
            "security.protocol": "PLAINTEXT",
            "socket.keepalive.enable": True,
            "message.timeout.ms": 30000,
        }
        config.update(defaults)

        # allow overriding with user-provided extras
        config.update(self.extra_config)
        return config

    def _consume(self):
        consumer = Consumer(self._build_config())
        consumer.subscribe([self.topic])

        logger.info(f"Kafka consumer started for topic '{self.topic}'")

        try:
            while self._running:
                msg = consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                if msg.error():
                    logger.error(f"Consumer error: {msg.error()}")
                    continue

                try:
                    data = msg.value().decode() if isinstance(msg.value(), bytes) else msg.value()
                    parsed = json.loads(data)
                    logger.info(f"Consumed message: {parsed}")
                    if self.callback:
                        self.callback(parsed)
                except Exception as e:
                    logger.error(f"Error processing message: {e}")

        finally:
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


# from confluent_kafka import Consumer
# import threading
# import json
# import logging

# logger = logging.getLogger(__name__)

# class KafkaConsumerService:
#     def __init__(self, topic, bootstrap_servers="localhost:9092", group_id="default-group", auto_offset_reset="earliest", enable_auto_commit=True, callback=None):
#         self.topic = topic
#         self.bootstrap_servers = bootstrap_servers
#         self.group_id = group_id
#         self.auto_offset_reset = auto_offset_reset
#         self.enable_auto_commit = enable_auto_commit
#         self.callback = callback
#         self._thread = None
#         self._running = False

#     def _consume(self):
#         consumer = Consumer(
#             self.topic,
#             bootstrap_servers=self.bootstrap_servers,
#             group_id=self.group_id,
#             auto_offset_reset=self.auto_offset_reset,
#             enable_auto_commit=self.enable_auto_commit,
            
#         )

#         logger.info(f"Kafka consumer started for topic '{self.topic}'")

#         for message in consumer:
#             try:
#                 data = message.value.decode() if isinstance(message.value, bytes) else message.value
#                 parsed = json.loads(data)
#                 logger.info(f"Consumed message: {parsed}")
#                 if self.callback:
#                     self.callback(parsed)
#             except Exception as e:
#                 logger.error(f"Error processing message: {e}")

#             if not self._running:
#                 break

#         consumer.close()

#     def start(self):
#         if not self._thread or not self._thread.is_alive():
#             self._running = True
#             self._thread = threading.Thread(target=self._consume, daemon=True)
#             self._thread.start()

#     def stop(self):
#         self._running = False
#         if self._thread:
#             self._thread.join()
