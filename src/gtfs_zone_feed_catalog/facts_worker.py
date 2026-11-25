"""The facts of one zip on disk, computed in a worker process of its own.

Only the library is imported here: a worker is a fresh process capped in
address space, and Dagster or obstore (whose runtime threads reserve over a GB)
would count against the cap."""

import csv
import zipfile
from pathlib import Path
from typing import Any

from gtfs_zone_feed_fetch.facts import FACTS_VERSION, scan_fingerprinted
from gtfs_zone_feed_fetch.fingerprint import to_parquet
from gtfs_zone_feed_fetch.localities import Localities
from gtfs_zone_feed_fetch.reader import MissingFiles

DETAIL_LENGTH = 200


def facts_doc(
    outcome: str, detail: str = "", facts: dict[str, Any] | None = None
) -> dict[str, Any]:
    """A ``gtfs/<sha256>.facts.json`` document. ``facts`` is set when ok."""
    return {
        "version": FACTS_VERSION,
        "outcome": outcome,
        "detail": detail[:DETAIL_LENGTH],
        "facts": facts or {},
    }


def scan_doc(path: str, localities: str | None = None) -> tuple[dict[str, Any], bytes]:
    """The facts document of the zip at ``path``, with municipality tags from
    the locality file at ``localities`` when given, and its
    ``gtfs/<sha256>.fingerprint.parquet`` file, b"" unless the outcome is ok. A
    zip that cannot be read is an outcome."""
    try:
        found = Localities(localities) if localities else None
        facts, fingerprint = scan_fingerprinted(Path(path), found)
        return facts_doc("ok", facts=facts), to_parquet(fingerprint)
    except zipfile.BadZipFile as exc:
        return facts_doc("not_zip", str(exc)), b""
    except MissingFiles as exc:
        return facts_doc("missing_files", str(exc)), b""
    except (
        ValueError,
        KeyError,
        IndexError,
        UnicodeDecodeError,
        csv.Error,
        EOFError,
    ) as exc:
        return facts_doc("parse_error", f"{type(exc).__name__}: {exc}"), b""
