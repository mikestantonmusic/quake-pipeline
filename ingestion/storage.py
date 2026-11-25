"""Where raw files are written: the local landing zone, an S3 bucket, or both.

Every store takes the same relative key, e.g. ``raw/feed/all_day/2026/11/25/<ts>.geojson``,
so a file has the same name locally and in S3. Writing the same key again overwrites it,
which keeps re-runs idempotent in both places.

RAW_STORAGE picks the stores (comma-separated): ``local`` (default), ``s3``, or ``local,s3``.
S3 needs RAW_BUCKET; credentials come from the standard AWS variables or profile.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Protocol

GEOJSON_CONTENT_TYPE = "application/geo+json"


class RawStore(Protocol):
    def write(self, key: str, body: bytes) -> str:
        """Save body under key and return where it went (a local path or an s3:// URI)."""
        ...


class LocalStore:
    """Files under a local directory (the ``data/`` folder)."""

    def __init__(self, root: Path | str) -> None:
        self.root = Path(root)

    def write(self, key: str, body: bytes) -> str:
        path = self.root / key
        path.parent.mkdir(parents=True, exist_ok=True)
        # Write to a temp file then rename, so a crash never leaves a half-written file.
        tmp = path.with_suffix(path.suffix + ".tmp")
        tmp.write_bytes(body)
        os.replace(tmp, path)
        return str(path)


class S3Store:
    """Objects in an S3 bucket. An S3 upload is all-or-nothing, so no temp file is needed."""

    def __init__(self, bucket: str, client=None) -> None:
        self.bucket = bucket
        self._client = client

    @property
    def client(self):
        if self._client is None:
            import boto3
            from botocore.config import Config

            # "standard" retries throttling and transient network errors with backoff.
            self._client = boto3.client("s3", config=Config(retries={"mode": "standard"}))
        return self._client

    def write(self, key: str, body: bytes) -> str:
        self.client.put_object(
            Bucket=self.bucket, Key=key, Body=body, ContentType=GEOJSON_CONTENT_TYPE
        )
        return f"s3://{self.bucket}/{key}"


def stores_from_env(data_dir: Path | str) -> list[RawStore]:
    """Build the stores named in RAW_STORAGE."""
    names = [n.strip() for n in os.environ.get("RAW_STORAGE", "local").split(",") if n.strip()]
    stores: list[RawStore] = []
    for name in names:
        if name == "local":
            stores.append(LocalStore(data_dir))
        elif name == "s3":
            bucket = os.environ.get("RAW_BUCKET")
            if not bucket:
                raise ValueError("RAW_STORAGE includes s3 but RAW_BUCKET is not set")
            stores.append(S3Store(bucket))
        else:
            raise ValueError(f"Unknown RAW_STORAGE entry {name!r}; use local and/or s3")
    if not stores:
        raise ValueError("RAW_STORAGE is empty; use local and/or s3")
    return stores
