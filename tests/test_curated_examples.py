"""The curated set, and the notes that are the expensive part of it."""

from geometry_car.assets.curated_examples import example_sources, load_examples


def test_every_example_has_a_name_and_a_scheduled_url():
    for example in load_examples():
        assert example.name
        assert example.scheduled.get("url")


def test_no_plain_http_entry_is_published_without_the_proxy():
    # An http URL on an https page is mixed content; these entries work only
    # because maybeProxy makes the plain-http hop server-side.
    for example in load_examples():
        for half in (example.scheduled, example.realtime):
            urls = [
                v
                for k, v in half.items()
                if k.endswith("url") or k in ("vehicles", "trip_updates", "alerts")
            ]
            if any(str(url).startswith("http://") for url in urls):
                assert half.get("use_cors") is True, example.slug


def test_the_proxy_only_hosts_keep_their_notes():
    notes = {e.slug: e.note for e in load_examples()}
    assert "403" in notes["ripta"]
    assert "proxy" in notes["burlington-transit"]
    # The GitHub rewrite rule is the one that costs an afternoon to rediscover.
    assert "raw.githubusercontent.com" in notes["columbia-county"]


def test_rows_split_into_static_and_rt_and_skip_the_missing_half():
    rows = {r.source_id for r in example_sources(load_examples())}
    assert "curated:amtrak:static" in rows
    assert "curated:amtrak:rt" in rows
    # West Bus Service publishes no GTFS-RT, so there is no rt row to check.
    assert "curated:west-bus-service:static" in rows
    assert "curated:west-bus-service:rt" not in rows
