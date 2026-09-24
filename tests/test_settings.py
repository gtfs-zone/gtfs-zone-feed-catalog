from geometry_car.settings import Settings


def test_defaults_are_polite():
    s = Settings(_env_file=None)
    assert s.check_concurrency > 0
    assert s.check_per_host_delay_seconds > 0
    assert "list.gtfs.zone" in s.check_user_agent


def test_secrets_have_no_baked_defaults():
    s = Settings(_env_file=None)
    assert s.mobility_db_refresh_token == ""
    assert s.database_url == ""
    assert s.gatus_token == ""
