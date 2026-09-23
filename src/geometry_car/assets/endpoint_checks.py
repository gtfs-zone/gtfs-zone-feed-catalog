"""Does the endpoint still answer?

Ten thousand outbound requests a day from one home IP is a monitor only while
it stays polite, so the checker is bounded three ways: a global cap on requests
in flight, one request at a time per host with a pause between them, and a
User-Agent that names the project and where to complain. None of those are
tuning knobs to raise casually.

HEAD first, because a feed is a zip or a protobuf and nobody needs the body. A
fair number of origins reject HEAD outright, so a rejection falls back to a GET
with ``Range: bytes=0-0``, streamed and closed after the headers - the response
body is never read. No feed contents are stored, ever.

Redirects are followed, and a final URL that differs from the requested one is
recorded: a catalog entry pointing at a 301 is itself a finding.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime

import httpx
from dagster import asset

from geometry_car.catalog import Source
from geometry_car.settings import settings
from geometry_car.urls import host_of, normalize_url


def result_key(url: str) -> str:
    """How a URL is looked up in a results map.

    Absolute URLs collapse onto their normalized form, so two catalogs listing
    the same endpoint share one check. A path-only URL has no normalized form
    and stands for itself.
    """
    return normalize_url(url) or url


log = logging.getLogger(__name__)

# Statuses that mean "this origin does not do HEAD", not "this feed is gone".
HEAD_REJECTED = frozenset({400, 401, 403, 404, 405, 406, 409, 429, 500, 501, 502, 503})


@dataclass(frozen=True, slots=True)
class CheckResult:
    """One endpoint, checked once. Keyed by normalized URL, not by source."""

    url: str
    ok: bool
    checked_at: datetime
    status_code: int | None = None
    # Only set when the request landed somewhere other than where it was aimed.
    final_url: str = ""
    content_type: str = ""
    content_length: int | None = None
    latency_ms: int | None = None
    # "" when ok. Otherwise one of: dns, tls, timeout, refused, http_4xx,
    # http_5xx, other - or a skip reason: auth_required, relative.
    error_class: str = ""
    method: str = ""
    skipped: bool = False


def _error_class(exc: Exception) -> str:
    if isinstance(exc, httpx.ConnectTimeout | httpx.ReadTimeout | httpx.PoolTimeout):
        return "timeout"
    if isinstance(exc, httpx.ConnectError):
        message = str(exc).lower()
        # httpx folds name resolution, TLS and refusal into ConnectError, so the
        # only thing to go on is the message. Coarse on purpose.
        if "name or service not known" in message or "nodename nor servname" in message:
            return "dns"
        if "refused" in message:
            return "refused"
        return "refused"
    if isinstance(exc, httpx.ProxyError | httpx.UnsupportedProtocol):
        return "other"
    if "ssl" in type(exc).__name__.lower() or "certificate" in str(exc).lower():
        return "tls"
    return "other"


class _Politeness:
    """One request at a time per host, with a delay between them."""

    def __init__(self, delay: float) -> None:
        self._delay = delay
        self._locks: dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)
        self._last: dict[str, float] = {}

    def lock(self, host: str) -> asyncio.Lock:
        return self._locks[host]

    async def wait(self, host: str) -> None:
        elapsed = time.monotonic() - self._last.get(host, 0.0)
        if elapsed < self._delay:
            await asyncio.sleep(self._delay - elapsed)

    def done(self, host: str) -> None:
        self._last[host] = time.monotonic()


async def _check_one(
    client: httpx.AsyncClient,
    url: str,
    politeness: _Politeness,
    slots: asyncio.Semaphore,
) -> CheckResult:
    # A fragment is the app's own nested-zip selector, never the server's.
    request_url = url.split("#", 1)[0].strip()
    host = host_of(request_url)

    # Host lock first, global slot second: a task queued behind a busy host
    # must not hold one of the global slots while it waits.
    async with politeness.lock(host):
        await politeness.wait(host)
        await slots.acquire()
        # Latency is the request alone, not the time spent queued.
        started = time.monotonic()
        try:
            response = await client.head(request_url)
            method = "HEAD"
            if response.status_code in HEAD_REJECTED:
                response = await _ranged_get(client, request_url)
                method = "GET"
        except Exception as exc:
            # One unusable URL is a failed check, never a failed run.
            if not isinstance(exc, httpx.HTTPError):
                log.warning("check of %r raised %s: %s", url, type(exc).__name__, exc)
            return CheckResult(
                url=url,
                ok=False,
                checked_at=datetime.now(UTC),
                latency_ms=int((time.monotonic() - started) * 1000),
                error_class=_error_class(exc),
                method="HEAD",
            )
        finally:
            slots.release()
            politeness.done(host)

    status = response.status_code
    ok = status < 400
    final_url = str(response.url)
    length = response.headers.get("content-length")
    return CheckResult(
        url=url,
        ok=ok,
        checked_at=datetime.now(UTC),
        status_code=status,
        final_url="" if final_url == request_url else final_url,
        content_type=response.headers.get("content-type", "").split(";")[0].strip(),
        content_length=int(length) if length and length.isdigit() else None,
        latency_ms=int((time.monotonic() - started) * 1000),
        error_class="" if ok else f"http_{status // 100}xx",
        method=method,
    )


async def _ranged_get(client: httpx.AsyncClient, url: str) -> httpx.Response:
    """A GET whose body is thrown away after the headers arrive."""
    request = client.build_request("GET", url, headers={"Range": "bytes=0-0"})
    response = await client.send(request, stream=True, follow_redirects=True)
    await response.aclose()
    return response


def check_targets(rows: list[Source]) -> tuple[dict[str, str], dict[str, CheckResult]]:
    """Split every row's URLs into what to check and what to skip, by normalized key.

    Returns the checkable targets (normalized key -> one raw URL to request) and
    the already-decided skips. A URL that only ever appears on a feed needing an
    API key we do not hold is skipped rather than checked into a guaranteed 401.
    """
    targets: dict[str, str] = {}
    needs_auth: dict[str, str] = {}
    relative: dict[str, str] = {}

    for row in rows:
        for role in row.roles:
            url = row.urls.get(role, "")
            if not url:
                continue
            key = normalize_url(url)
            if not key:
                # Path-only curated realtime URLs; they resolve against each
                # app's own RT base and there is no one host to check.
                relative.setdefault(url, url)
                continue
            if row.authentication_type:
                needs_auth.setdefault(key, url)
            else:
                targets.setdefault(key, url)

    now = datetime.now(UTC)
    skipped = {
        key: CheckResult(
            url=url, ok=False, checked_at=now, error_class="auth_required", skipped=True
        )
        for key, url in needs_auth.items()
        if key not in targets
    }
    skipped |= {
        url: CheckResult(
            url=url, ok=False, checked_at=now, error_class="relative", skipped=True
        )
        for url in relative
    }
    return targets, skipped


async def run_checks(targets: dict[str, str]) -> dict[str, CheckResult]:
    politeness = _Politeness(settings.check_per_host_delay_seconds)
    slots = asyncio.Semaphore(settings.check_concurrency)
    timeout = httpx.Timeout(settings.check_timeout_seconds)

    async with httpx.AsyncClient(
        headers={"User-Agent": settings.check_user_agent},
        timeout=timeout,
        follow_redirects=True,
        # A feed host that stalls must not hold a connection open for the rest
        # of the run.
        limits=httpx.Limits(max_connections=settings.check_concurrency),
    ) as client:

        async def one(key: str, url: str) -> tuple[str, CheckResult]:
            return key, await _check_one(client, url, politeness, slots)

        results = await asyncio.gather(*(one(k, u) for k, u in targets.items()))

    return dict(results)


@asset(description="Reachability of every distinct endpoint, keyed by normalized URL")
def endpoint_checks(sources: list[Source]) -> dict[str, CheckResult]:
    targets, skipped = check_targets(sources)
    log.info("checking %d distinct endpoints, skipping %d", len(targets), len(skipped))

    results = asyncio.run(run_checks(targets))
    results |= skipped

    reachable = sum(1 for r in results.values() if r.ok)
    log.info("%d/%d endpoints answered", reachable, len(targets))
    return results
