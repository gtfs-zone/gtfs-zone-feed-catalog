import pytest
from gtfs_zone_db_models.object_store import ObjectStore
from obstore.store import MemoryStore

from gtfs_zone_feed_catalog.assets import localities as module
from gtfs_zone_feed_catalog.settings import settings
from gtfs_zone_feed_catalog.zips import ZipStoreSettings


@pytest.fixture
def store(tmp_path, monkeypatch):
    monkeypatch.setattr(settings, "fetch_workdir", str(tmp_path))
    return ObjectStore(ZipStoreSettings(s3_endpoint=""), MemoryStore())


def fake_build(calls):
    def build(source, out, workdir, memory_limit):
        assert memory_limit == settings.localities_memory
        calls.append(source)
        out.write_bytes(b"PAR1")
        return 7

    return build


def test_builds_a_new_release_then_skips_it(store):
    calls = []
    first = module.refresh(store, "2026-11-18.0", fake_build(calls))
    assert first == {"release": "2026-11-18.0", "skipped": False, "rows": 7, "bytes": 4}
    assert calls == [module.overture_source("2026-11-18.0")]
    assert store.get(module.LOCALITIES_KEY) == b"PAR1"
    assert store.get(module.RELEASE_KEY) == b"2026-11-18.0"

    again = module.refresh(store, "2026-11-18.0", fake_build(calls))
    assert again == {"release": "2026-11-18.0", "skipped": True}
    assert len(calls) == 1

    module.refresh(store, "2026-12-16.0", fake_build(calls))
    assert store.get(module.RELEASE_KEY) == b"2026-12-16.0"


def test_a_failed_build_keeps_the_stored_release(store):
    module.refresh(store, "2026-11-18.0", fake_build([]))

    def broken(source, out, workdir, memory_limit):
        raise RuntimeError("no network")

    with pytest.raises(RuntimeError):
        module.refresh(store, "2026-12-16.0", broken)
    assert store.get(module.RELEASE_KEY) == b"2026-11-18.0"


def test_no_endpoint_builds_nothing(monkeypatch):
    monkeypatch.setenv("ZIPS_S3_ENDPOINT", "")
    monkeypatch.setattr(
        module, "latest_overture_release", lambda: pytest.fail("listed Overture")
    )
    assert module.localities().metadata["skipped"] is True
