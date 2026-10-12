import logging

logger = logging.getLogger("validationhelper")


def skip_if_missing(handler, s3_writer=None):
    """
    Wrap a KafkaConsumerService callback so messages whose batch, file doc or
    S3 object no longer exist are logged and skipped. Returning without calling
    `handler` makes the consumer commit the offset instead of retrying forever.
    Lookup errors (connection failures, etc.) still raise, so those are retried.
    Pass `s3_writer` to also check the S3 object; omit it for jobs with no s3UL.
    """
    def wrapped(document: dict, batch: dict, raw_value: bytes, ctx: dict) -> None:
        raw_text = raw_value.decode("utf-8", errors="replace")
        batch_id = ctx.get("batch_id")
        file_id = ctx.get("file_id")

        if not batch_id or not batch:
            logger.warning(f"Batch {batch_id} not found in MongoDB, skipping. raw_message={raw_text}")
            return

        if not file_id or not ctx["mongo_writer"].get(file_id, "files"):
            logger.warning(f"File {file_id} not found in MongoDB, skipping. raw_message={raw_text}")
            return

        if s3_writer is not None:
            s3url = document.get("s3UL")
            if not s3url or not s3_writer.file_exists(s3url):
                logger.warning(f"File not found in S3 ({s3url}), skipping. raw_message={raw_text}")
                return

        return handler(document, batch, raw_value, ctx)

    return wrapped