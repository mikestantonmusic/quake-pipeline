import pytest

from ingestion.storage import LocalStore, S3Store, stores_from_env

KEY = "raw/feed/all_day/2026/11/25/20261125T120000Z.geojson"


def test_local_store_writes_under_root(tmp_path):
    location = LocalStore(tmp_path).write(KEY, b'{"a": 1}')

    assert location == str(tmp_path / KEY)
    assert (tmp_path / KEY).read_bytes() == b'{"a": 1}'
    assert not list(tmp_path.rglob("*.tmp"))


def test_local_store_overwrites_same_key(tmp_path):
    store = LocalStore(tmp_path)
    store.write(KEY, b"first")
    store.write(KEY, b"second")

    assert (tmp_path / KEY).read_bytes() == b"second"
    assert len(list(tmp_path.rglob("*.geojson"))) == 1


def test_s3_store_uploads_object(s3, s3_store):
    location = s3_store.write(KEY, b'{"a": 1}')

    assert location == f"s3://{s3_store.bucket}/{KEY}"
    obj = s3.get_object(Bucket=s3_store.bucket, Key=KEY)
    assert obj["Body"].read() == b'{"a": 1}'
    assert obj["ContentType"] == "application/geo+json"


def test_s3_store_overwrites_same_key(s3, s3_store):
    s3_store.write(KEY, b"first")
    s3_store.write(KEY, b"second")

    listing = s3.list_objects_v2(Bucket=s3_store.bucket)
    assert [o["Key"] for o in listing["Contents"]] == [KEY]
    assert s3.get_object(Bucket=s3_store.bucket, Key=KEY)["Body"].read() == b"second"


def test_stores_from_env_defaults_to_local(monkeypatch, tmp_path):
    monkeypatch.delenv("RAW_STORAGE", raising=False)

    stores = stores_from_env(tmp_path)

    assert len(stores) == 1 and isinstance(stores[0], LocalStore)


def test_stores_from_env_local_and_s3(monkeypatch, tmp_path):
    monkeypatch.setenv("RAW_STORAGE", "local, s3")
    monkeypatch.setenv("RAW_BUCKET", "some-bucket")

    local, s3 = stores_from_env(tmp_path)

    assert isinstance(local, LocalStore) and local.root == tmp_path
    assert isinstance(s3, S3Store) and s3.bucket == "some-bucket"


@pytest.mark.parametrize(
    ("storage", "bucket", "message"),
    [
        ("s3", None, "RAW_BUCKET is not set"),
        ("gcs", None, "Unknown RAW_STORAGE entry 'gcs'"),
        (" , ", None, "RAW_STORAGE is empty"),
    ],
)
def test_stores_from_env_rejects_bad_config(monkeypatch, tmp_path, storage, bucket, message):
    monkeypatch.setenv("RAW_STORAGE", storage)
    monkeypatch.delenv("RAW_BUCKET", raising=False)

    with pytest.raises(ValueError, match=message):
        stores_from_env(tmp_path)
