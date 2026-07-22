import os
import sys
import json
import threading
from confluent_kafka import Consumer, KafkaException
from shared_lib.mongo_connector import MongoWriter


class KafkaConsumerService:
    """
    Handles Kafka/Redpanda connection, Mongo bookkeeping, polling, committing,
    and rollback-on-failure. Your business logic lives entirely in the
    `callback` function you pass in.

        def handle_kafka_message(document: dict, batch: dict, raw_value: bytes) -> None:
            ...

        consumer = KafkaConsumerService(
            topic="excel",
            bootstrap_servers=bootstrap_servers,
            group_id="Data-Extractor-excel",
            callback=handle_kafka_message,
        )
        sys.exit(consumer.start())
    """

    def __init__(
        self,
        topic: str,
        bootstrap_servers: str,
        group_id: str,
        callback,
        mongodb_url: str = None,
        mongo_db: str = "AI-DB",
        mongo_collection: str = "messages",
        mongo_tls_cert_key_file: str = None,
        mongo_tls_ca_file: str = None,
        poll_timeout_sec: float = None,
        max_poll_interval_ms: int = None,
        session_timeout_ms: int = None,
        sasl_username: str = None,
        sasl_password: str = None,
        sasl_mechanism: str = None,
    ):
        if callback is None:
            raise ValueError("callback is required")

        self.callback = callback
        self.topic = topic
        self.group_id = group_id

        self.poll_timeout_sec = poll_timeout_sec or float(
            os.environ.get("POLL_TIMEOUT_SEC", "10")
        )
        self.mongodb_url = mongodb_url or os.environ["MONGODB_URL"]
        self.mongo_db = mongo_db
        self.mongo_collection = mongo_collection
        self.mongo_tls_cert_key_file = mongo_tls_cert_key_file or os.environ.get(
            "MONGO_TLS_CERT_KEY_FILE"
        )
        self.mongo_tls_ca_file = mongo_tls_ca_file or os.environ.get("MONGO_TLS_CA_FILE")

        self.conf = {
            "bootstrap.servers": bootstrap_servers,
            "group.id": group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "max.poll.interval.ms": max_poll_interval_ms
            or int(os.environ.get("MAX_POLL_INTERVAL_MS", str(90 * 60 * 1000))),
            "session.timeout.ms": session_timeout_ms
            or int(os.environ.get("SESSION_TIMEOUT_MS", "45000")),
        }

        sasl_username = sasl_username or os.environ.get("REDPANDA_SASL_USERNAME")
        if sasl_username:
            self.conf.update(
                {
                    "security.protocol": "SASL_SSL",
                    "sasl.mechanism": sasl_mechanism
                    or os.environ.get("REDPANDA_SASL_MECHANISM", "SCRAM-SHA-256"),
                    "sasl.username": sasl_username,
                    "sasl.password": sasl_password
                    or os.environ.get("REDPANDA_SASL_PASSWORD"),
                }
            )

        self.mongo_writer: MongoWriter = None
        self.consumer: Consumer = None
        self._stop_heartbeat = threading.Event()

    def _heartbeat_loop(self):
        # Placeholder thread, kept for parity with the original script in
        # case a future client lib needs manual heartbeats.
        while not self._stop_heartbeat.wait(30):
            pass

    def _rollback(self, inserted_id) -> None:
        """
        Best-effort cleanup after a failed callback. Deletes the document we
        just inserted so a retry (message redelivery, since we don't commit
        the offset) doesn't leave duplicates behind.
        """
        if inserted_id is None:
            return
        try:
            self.mongo_writer.delete(inserted_id)
            print(f"Rolled back inserted document {inserted_id}.", flush=True)
        except Exception as rollback_e:
            print(
                f"CRITICAL: rollback failed for document {inserted_id}: {rollback_e}",
                file=sys.stderr,
                flush=True,
            )

    def start(self) -> int:
        self.mongo_writer = MongoWriter(
            uri=self.mongodb_url,
            db=self.mongo_db,
            collection=self.mongo_collection,
            tls_cert_key_file=self.mongo_tls_cert_key_file,
            tls_ca_file=self.mongo_tls_ca_file,
        )
        self.mongo_writer.connect()

        self.consumer = Consumer(self.conf)
        self.consumer.subscribe([self.topic])

        hb_thread = threading.Thread(target=self._heartbeat_loop, daemon=True)
        hb_thread.start()

        exit_code = 0
        inserted_id = None

        try:
            msg = self.consumer.poll(timeout=self.poll_timeout_sec)

            if msg is None:
                print("No message available — exiting cleanly.", flush=True)

            elif msg.error():
                raise KafkaException(msg.error())

            else:
                document = json.loads(msg.value())

                try:
                    # --- DB + processing work: all-or-nothing ---
                    inserted_id = self.mongo_writer.insert(document)
                    batch_id = document.get("BatchId")
                    batch = self.mongo_writer.get(batch_id, "batch")
                    file_id = document.get("FileId")

                    ctx = {
                        "mongo_writer": self.mongo_writer,
                        "file_id": file_id,
                        "batch_id": document.get("BatchId"),
                    }
                    self.callback(document, batch, msg.value(), ctx)
                    # self.callback(document, batch, msg.value())

                except Exception as inner_e:
                    # Something failed mid-way: roll back the insert so we
                    # don't leave a partial/duplicate record behind, and do
                    # NOT commit the offset so this message is reprocessed.
                    print(
                        f"Processing failed, rolling back DB changes: {inner_e}",
                        file=sys.stderr,
                        flush=True,
                    )
                    self._rollback(inserted_id)
                    raise  # re-raise so the outer except sets exit_code = 1

                # Only reached if insert + get + callback all succeeded
                self.consumer.commit(msg, asynchronous=False)
                print("Message committed. Job exiting.", flush=True)

        except Exception as e:
            print(f"Error processing message: {e}", file=sys.stderr, flush=True)
            exit_code = 1

        finally:
            self._stop_heartbeat.set()
            self.consumer.close()
            self.mongo_writer.close()

        return exit_code

# from confluent_kafka import Consumer
# import threading
# import json
# import logging

# logger = logging.getLogger(__name__)

# class KafkaConsumerService:
#     def __init__(
#         self,
#         topic,
#         bootstrap_servers="localhost:9092",
#         group_id="default-group",
#         auto_offset_reset="earliest",
#         enable_auto_commit=True,
#         callback=None,
#         extra_config=None,   # optional dict
#     ):
#         self.topic = topic
#         self.bootstrap_servers = bootstrap_servers
#         self.group_id = group_id
#         self.auto_offset_reset = auto_offset_reset
#         self.enable_auto_commit = enable_auto_commit
#         self.callback = callback
#         self.extra_config = extra_config or {}
#         self._thread = None
#         self._running = False

#     def _build_config(self):
#         # base config
#         config = {
#             "bootstrap.servers": self.bootstrap_servers,
#             "group.id": self.group_id,
#             "auto.offset.reset": self.auto_offset_reset,
#             "enable.auto.commit": self.enable_auto_commit,
#         }

#         # add your defaults here
#         defaults = {
#             "security.protocol": "PLAINTEXT",
#             "socket.keepalive.enable": True,
#             "message.timeout.ms": 30000,
#         }
#         config.update(defaults)

#         # allow overriding with user-provided extras
#         config.update(self.extra_config)
#         return config

#     def _consume(self):
#         consumer = Consumer(self._build_config())
#         consumer.subscribe([self.topic])

#         logger.info(f"Kafka consumer started for topic '{self.topic}'")

#         try:
#             while self._running:
#                 msg = consumer.poll(timeout=1.0)
#                 if msg is None:
#                     continue
#                 if msg.error():
#                     logger.error(f"Consumer error: {msg.error()}")
#                     continue

#                 try:
#                     data = msg.value().decode() if isinstance(msg.value(), bytes) else msg.value()
#                     parsed = json.loads(data)
#                     logger.info(f"Consumed message: {parsed}")
#                     if self.callback:
#                         self.callback(parsed)
#                 except Exception as e:
#                     logger.error(f"Error processing message: {e}")

#         finally:
#             consumer.close()

#     def start(self):
#         if not self._thread or not self._thread.is_alive():
#             self._running = True
#             self._thread = threading.Thread(target=self._consume, daemon=True)
#             self._thread.start()

#     def stop(self):
#         self._running = False
#         if self._thread:
#             self._thread.join()


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
