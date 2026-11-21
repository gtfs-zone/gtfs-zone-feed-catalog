"""Post every failed Dagster run, in any code location, to Telegram."""

import logging

import httpx
from dagster import DefaultSensorStatus, RunFailureSensorContext, run_failure_sensor

from gtfs_zone_feed_catalog.settings import Settings, settings

log = logging.getLogger(__name__)


def failure_message(
    job: str, location: str | None, run_id: str, error: str | None, dagster_url: str
) -> str:
    """Job, location, the first line of the error and a link to the run."""
    first_line = (error or "").strip().splitlines()[0:1]
    lines = [f"Dagster run failed: {job} ({location or 'unknown location'})"]
    if first_line:
        lines.append(first_line[0][:300])
    lines.append(f"{dagster_url.rstrip('/')}/runs/{run_id}")
    return "\n".join(lines)


def send_telegram(
    settings: Settings, text: str, client: httpx.Client | None = None
) -> bool:
    """POST the text to the chat. Returns whether it was accepted; never raises."""
    if not settings.telegram_bot_token or not settings.telegram_chat_id:
        log.info("TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID unset; not posting")
        return False
    url = f"https://api.telegram.org/bot{settings.telegram_bot_token}/sendMessage"
    try:
        with client or httpx.Client(timeout=10.0) as http:
            response = http.post(
                url,
                json={
                    "chat_id": settings.telegram_chat_id,
                    "text": text,
                    "disable_web_page_preview": True,
                },
            )
            response.raise_for_status()
    except httpx.HTTPError as exc:
        # The exception text holds the URL, and so the bot token.
        log.warning("telegram post failed: %s", type(exc).__name__)
        return False
    return True


@run_failure_sensor(
    name="run_failure_alert",
    monitor_all_code_locations=True,
    default_status=DefaultSensorStatus.RUNNING,
    description="Post each failed run in any code location to Telegram",
)
def run_failure_alert(context: RunFailureSensorContext) -> None:
    run = context.dagster_run
    origin = run.remote_job_origin
    location = origin.location_name if origin else None
    error = context.failure_event.message
    send_telegram(
        settings,
        failure_message(
            run.job_name, location, run.run_id, error, settings.dagster_url
        ),
    )
