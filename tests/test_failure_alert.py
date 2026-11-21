"""The run-failure post: its message, its request, and that it never raises."""

import json

import httpx
import respx

from gtfs_zone_feed_catalog.failure_alert import failure_message, send_telegram
from gtfs_zone_feed_catalog.settings import Settings

POST = "https://api.telegram.org/botT/sendMessage"


def configured() -> Settings:
    return Settings(_env_file=None, telegram_bot_token="T", telegram_chat_id="42")


def test_message_has_job_location_error_and_link():
    text = failure_message(
        "daily_catalog",
        "feed_catalog",
        "abc",
        "Execution of run failed.\nTraceback...",
        "https://dagster.gtfs.zone/",
    )
    assert text == (
        "Dagster run failed: daily_catalog (feed_catalog)\n"
        "Execution of run failed.\n"
        "https://dagster.gtfs.zone/runs/abc"
    )


def test_message_without_error_or_location():
    text = failure_message("crimes_job", None, "r1", None, "https://d")
    assert (
        text == "Dagster run failed: crimes_job (unknown location)\nhttps://d/runs/r1"
    )


def test_no_token_skips_the_post():
    assert send_telegram(Settings(_env_file=None), "x") is False


@respx.mock
def test_posts_text_to_the_chat():
    route = respx.post(POST).respond(200, json={"ok": True})

    assert send_telegram(configured(), "hello") is True
    body = json.loads(route.calls.last.request.content)
    assert body["chat_id"] == "42"
    assert body["text"] == "hello"


@respx.mock
def test_a_rejected_post_is_swallowed():
    respx.post(POST).respond(401)
    assert send_telegram(configured(), "x") is False


@respx.mock
def test_an_unreachable_telegram_is_swallowed():
    respx.post(POST).mock(side_effect=httpx.ConnectError("refused"))
    assert send_telegram(configured(), "x") is False
