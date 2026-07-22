import boto3
import json
import logging
import mimetypes
from urllib.parse import urlparse

logger = logging.getLogger(__name__)


class S3Writer:
    def __init__(
        self,
        aws_access_key_id: str,
        aws_secret_access_key: str,
        region_name: str,
    ) -> None:
        try:
            self.client = boto3.client(
                "s3",
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key,
                region_name=region_name,
            )

            logger.info(f"S3Writer initialized (region: {region_name})")
        except Exception as e:
            logger.error(f"Failed to initialize S3 client: {e}")
            raise

    @staticmethod
    def _parse_s3_path(s3_path):
        parsed = urlparse(s3_path)
        if parsed.scheme != "s3" or not parsed.netloc:
            raise ValueError(f"Invalid S3 path: {s3_path}")
        bucket = parsed.netloc
        key = parsed.path.lstrip("/")
        if not key:
            raise ValueError(f"Invalid S3 path (missing key): {s3_path}")
        return bucket, key

    def get_file(self, file_s3_url: str):
        """
        Fetch a file directly from the given S3 URL
        (e.g. "s3://my-bucket/a/b/c/d/prompting.txt").

        Returns:
            - dict/list if the key ends in .json (parsed)
            - str if the key looks like text (.txt, .csv, .md, etc.)
            - bytes otherwise
            - None if the object doesn't exist or fetch fails
        """
        try:
            bucket, key = self._parse_s3_path(file_s3_url)

            response = self.client.get_object(Bucket=bucket, Key=key)
            body = response["Body"].read()

            guessed_type, _ = mimetypes.guess_type(key)

            if key.endswith(".json"):
                return json.loads(body.decode("utf-8"))
            if guessed_type and guessed_type.startswith("text"):
                return body.decode("utf-8")

            return body

        except self.client.exceptions.NoSuchKey:
            logger.warning(f"File not found: {file_s3_url}")
            return None
        except Exception as e:
            logger.error(f"Failed to get file {file_s3_url}: {e}")
            return None

    def get_rawfile(self, file_s3_url: str):
        """
        Fetch a file directly from the given S3 URL
        (e.g. "s3://my-bucket/a/b/c/d/prompting.txt").

        Returns:
            - bytes: the raw file content
            - None if the object doesn't exist or fetch fails
        """
        try:
            bucket, key = self._parse_s3_path(file_s3_url)

            response = self.client.get_object(Bucket=bucket, Key=key)
            body = response["Body"].read()

            return body

        except self.client.exceptions.NoSuchKey:
            logger.warning(f"File not found: {file_s3_url}")
            return None
        except Exception as e:
            logger.error(f"Failed to get file {file_s3_url}: {e}")
            return None

    def save_result(self, result, file_s3_url: str, result_filename: str = "result.json") -> None:
        """
        Save `result` next to the given source file's S3 URL — i.e. in the
        same "folder" (key prefix), under `result_filename`.

        Example:
            file_s3_url = "s3://my-bucket/a/b/c/d/prompting.txt"
            -> result is written to "s3://my-bucket/a/b/c/d/result.json"

        - dict/list -> JSON-serialized
        - str       -> UTF-8 encoded as-is
        - bytes     -> written as-is
        """
        if result is None:
            logger.warning("Skipping save_result(): result is None")
            return

        try:
            bucket, file_key = self._parse_s3_path(file_s3_url)

            # Swap out the filename, keep the same folder prefix
            prefix = file_key.rsplit("/", 1)[0] + "/" if "/" in file_key else ""
            key = prefix + result_filename

            guessed_type, _ = mimetypes.guess_type(key)

            if isinstance(result, bytes):
                body = result
                content_type = guessed_type or "application/octet-stream"
            elif isinstance(result, str):
                body = result.encode("utf-8")
                content_type = guessed_type or "text/plain"
            else:
                body = json.dumps(result, default=str).encode("utf-8")
                content_type = guessed_type or "application/json"

            self.client.put_object(
                Bucket=bucket,
                Key=key,
                Body=body,
                ContentType=content_type,
            )
            logger.info(f"Saved result to s3://{bucket}/{key}")

        except Exception as e:
            logger.error(f"Failed to save result (source: {file_s3_url}): {e}")
            raise