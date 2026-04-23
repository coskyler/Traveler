import os
import boto3
from typing import Optional
from crawler.pipeline.types import FetchResult


class Storage:
    def put(self, key: str, data: FetchResult): ...
    def get(self, key: str) -> Optional[FetchResult]: ...


class InMemoryStorage(Storage):
    def __init__(self):
        self._store = {}

    def put(self, key, data):
        self._store[key] = data.model_dump_json()  # store as str

    def get(self, key):
        raw = self._store.get(key)
        return FetchResult.model_validate_json(raw) if raw else None


class S3Storage(Storage):
    def __init__(self, bucket: str):
        self.bucket = bucket
        self.s3 = boto3.client("s3")

    def put(self, key, data):
        self.s3.put_object(
            Bucket=self.bucket,
            Key=key,
            Body=data.model_dump_json().encode(),
        )

    def get(self, key):
        try:
            obj = self.s3.get_object(Bucket=self.bucket, Key=key)
            raw = obj["Body"].read().decode()
            return FetchResult.model_validate_json(raw)
        except self.s3.exceptions.NoSuchKey:
            return None


_storage = None

def get_storage() -> Storage:
    global _storage
    if _storage is None:
        if os.getenv("ENVIRONMENT") == "production":
            _storage = S3Storage(bucket=os.environ["S3_BUCKET"])
        else:
            _storage = InMemoryStorage()
    return _storage