"""MinIO / S3 access. Workers never share a filesystem; images and crops go through here.

Two things beyond plain get/put:

* Every call is timed into an IoTimer, so the runtime can split a task's handler time into
  fetch / infer / upload for the coordinator's overhead waterfall.
* Failures are translated into the runtime's error vocabulary: an unreachable or overloaded
  MinIO is an InfraError (the task is fine; release it and back off), while bytes that don't
  decode or an object that doesn't exist are NonRetryableErrors (retrying can't help).
"""

import io
import os
import time

import PIL.Image
import PIL.ImageFile
import PIL.ImageOps

from .runtime import InfraError, NonRetryableError

PIL.ImageFile.LOAD_TRUNCATED_IMAGES = True  # camera-trap JPEGs are sometimes truncated


class StorageUnavailable(InfraError):
    """MinIO could not be reached, timed out, or answered 5xx."""


class CorruptImage(NonRetryableError):
    """The object exists but is not a decodable image."""


class MissingObject(NonRetryableError):
    """The object key does not exist."""


# S3 error codes that mean "the service is struggling", not "your request is wrong".
_TRANSIENT_CODES = {"SlowDown", "ServiceUnavailable", "InternalError", "RequestTimeout", "XMinioServerNotInitialized"}
_MISSING_CODES = {"NoSuchKey", "NoSuchBucket", "404", "NotFound"}


def translate_s3_error(e: Exception, key: str) -> Exception:
    """Maps a botocore exception onto InfraError / NonRetryableError (or returns it unchanged)."""
    from botocore.exceptions import BotoCoreError, ClientError, ConnectionError as BotoConnectionError, HTTPClientError

    if isinstance(e, (BotoConnectionError, HTTPClientError)):  # endpoint down, connect/read timeouts
        return StorageUnavailable(f"S3 unreachable for {key}: {e}")
    if isinstance(e, ClientError):
        code = str(e.response.get("Error", {}).get("Code", ""))
        status = int(e.response.get("ResponseMetadata", {}).get("HTTPStatusCode", 0) or 0)
        if code in _MISSING_CODES or status == 404:
            return MissingObject(f"{key} does not exist ({code or status})")
        if status >= 500 or code in _TRANSIENT_CODES:
            return StorageUnavailable(f"S3 error for {key}: {code or status}")
    if isinstance(e, BotoCoreError):
        return StorageUnavailable(f"S3 client error for {key}: {e}")
    return e


def open_rgb(data: bytes) -> PIL.Image.Image:
    """Decode image bytes the same way speciesnet.utils.load_rgb_image does."""
    try:
        img = PIL.Image.open(io.BytesIO(data))
        img.load()
        img = img.convert("RGB")
    except (PIL.UnidentifiedImageError, PIL.Image.DecompressionBombError, SyntaxError) as e:
        raise CorruptImage(f"cannot decode image: {e}") from e
    return PIL.ImageOps.exif_transpose(img)


class IoTimer:
    """Milliseconds spent in storage reads and writes since the last reset()."""

    def __init__(self) -> None:
        self.fetch_ms = 0.0
        self.upload_ms = 0.0

    def reset(self) -> None:
        self.fetch_ms = 0.0
        self.upload_ms = 0.0


class Storage:
    def __init__(self, timer: IoTimer | None = None) -> None:
        import boto3
        from botocore.config import Config

        self.timer = timer or IoTimer()
        self.bucket = os.environ.get("S3_BUCKET", "wildebeest")
        self.s3 = boto3.client(
            "s3",
            endpoint_url=os.environ.get("S3_ENDPOINT", "http://minio:9000"),
            aws_access_key_id=os.environ.get("S3_ACCESS_KEY", "minioadmin"),
            aws_secret_access_key=os.environ.get("S3_SECRET_KEY", "minioadmin"),
            region_name="us-east-1",
            config=Config(
                signature_version="s3v4",
                s3={"addressing_style": "path"},
                retries={"max_attempts": 3},
                connect_timeout=3,
                read_timeout=10,
            ),
        )

    def get_image(self, key: str) -> PIL.Image.Image:
        """Download and decode; the time counts as fetchMs."""
        started = time.perf_counter()
        try:
            try:
                data = self.s3.get_object(Bucket=self.bucket, Key=key)["Body"].read()
            except Exception as e:
                raise translate_s3_error(e, key) from e
            return open_rgb(data)
        finally:
            self.timer.fetch_ms += (time.perf_counter() - started) * 1000

    def put_jpeg(self, key: str, data: bytes) -> None:
        started = time.perf_counter()
        try:
            self.s3.put_object(Bucket=self.bucket, Key=key, Body=data, ContentType="image/jpeg")
        except Exception as e:
            raise translate_s3_error(e, key) from e
        finally:
            self.timer.upload_ms += (time.perf_counter() - started) * 1000

    def ping(self) -> None:
        """Circuit-breaker probe: raises StorageUnavailable unless the bucket answers."""
        try:
            self.s3.head_bucket(Bucket=self.bucket)
        except Exception as e:
            err = translate_s3_error(e, self.bucket)
            raise err if isinstance(err, InfraError) else StorageUnavailable(str(e)) from e
