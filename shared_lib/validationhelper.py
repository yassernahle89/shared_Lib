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


def skip_if_batch_missing(handler):
    """
    Batch-only variant of skip_if_missing for jobs whose messages carry just a
    BatchId (no FileId / s3UL). Skips (so the consumer commits) when the batch
    doc is gone; lookup errors still raise and are retried.
    """
    def wrapped(document: dict, batch: dict, raw_value: bytes, ctx: dict) -> None:
        batch_id = ctx.get("batch_id")
        if not batch_id or not batch:
            logging.getLogger(__name__).warning(
                f"Batch {batch_id} not found in MongoDB, skipping. "
                f"raw_message={raw_value.decode('utf-8', errors='replace')}"
            )
            return
        return handler(document, batch, raw_value, ctx)

    return wrapped



def skip_if_inputs_missing(handler, s3_writer, input_exists):
    """
    For jobs whose message is {BatchId, Files: [...]}. Skips (consumer commits) if
    the batch doc is gone or Files is empty. If any file's input is missing from
    S3, marks the batch "Paused(MissingFile)" and skips. Lookup errors, including
    a failed status write, still raise, so the message is retried.
    input_exists(s3_writer, file, batch) -> bool says whether that file's input exists.
    """
    def wrapped(document: dict, batch: dict, raw_value: bytes, ctx: dict) -> None:
        log = logging.getLogger(__name__)
        raw_text = raw_value.decode("utf-8", errors="replace")
        batch_id = ctx.get("batch_id")

        if not batch_id or not batch:
            log.warning(f"Batch {batch_id} not found in MongoDB, skipping. raw_message={raw_text}")
            return

        files = document.get("Files")
        if isinstance(files, dict):
            files = [files]
        if not files:
            log.warning(f"No files found for BatchId={batch_id}, skipping. raw_message={raw_text}")
            return

        missing = [f.get("FileId") for f in files if not input_exists(s3_writer, f, batch)]
        if missing:
            log.warning(
                f"Inputs missing in S3 for BatchId={batch_id}, files={missing}; "
                f"pausing batch and skipping. raw_message={raw_text}"
            )
            ctx["mongo_writer"].update(batch_id, {"status": "Paused(MissingFile)"}, "batch")
            return

        return handler(document, batch, raw_value, ctx)

    return wrapped