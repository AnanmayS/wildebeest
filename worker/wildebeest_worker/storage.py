"""MinIO / S3 access. Workers never share a filesystem; images and crops go through here."""

import io
import os

import PIL.Image
import PIL.ImageFile
import PIL.ImageOps

PIL.ImageFile.LOAD_TRUNCATED_IMAGES = True  # camera-trap JPEGs are sometimes truncated


def open_rgb(data: bytes) -> PIL.Image.Image:
    """Decode image bytes the same way speciesnet.utils.load_rgb_image does."""
    img = PIL.Image.open(io.BytesIO(data))
    img.load()
    img = img.convert("RGB")
    return PIL.ImageOps.exif_transpose(img)


class Storage:
    def __init__(self) -> None:
        import boto3
        from botocore.config import Config

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
            ),
        )

    def get_image(self, key: str) -> PIL.Image.Image:
        obj = self.s3.get_object(Bucket=self.bucket, Key=key)
        return open_rgb(obj["Body"].read())

    def put_jpeg(self, key: str, data: bytes) -> None:
        self.s3.put_object(Bucket=self.bucket, Key=key, Body=data, ContentType="image/jpeg")
