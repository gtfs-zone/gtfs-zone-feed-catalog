"""Push a success result to a Gatus external endpoint."""

import logging

import httpx

from geometry_car.settings import Settings

log = logging.getLogger(__name__)


def push_heartbeat(settings: Settings, client: httpx.Client | None = None) -> bool:
    """POST success to Gatus. Returns whether it was accepted; never raises."""
    if not settings.gatus_url:
        log.info("GATUS_URL unset; not pushing a heartbeat")
        return False
    url = (
        f"{settings.gatus_url.rstrip('/')}/api/v1/endpoints/"
        f"{settings.gatus_endpoint_key}/external"
    )
    try:
        with client or httpx.Client(timeout=10.0) as http:
            response = http.post(
                url,
                params={"success": "true"},
                headers={"Authorization": f"Bearer {settings.gatus_token}"},
            )
            response.raise_for_status()
    except httpx.HTTPError as exc:
        # The artifacts are already up; a missed push only means Gatus pages
        # later, which is the safe direction to fail in.
        log.warning("gatus heartbeat push failed: %s", exc)
        return False
    return True
