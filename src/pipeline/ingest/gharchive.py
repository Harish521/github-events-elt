"""Ingest GH Archive hourly files into a partitioned bronze Parquet layer.

Design priorities (in order):
  1. Idempotency  -- rerunning the same hour must not duplicate data.
  2. Integrity    -- a partial/corrupt download must never be treated as success.
  3. Resilience   -- transient network failures are retried with backoff.

We land the raw event envelope with `payload` kept as a JSON string. We do NOT
flatten payloads here: there are 15 event types with 15 different payload
schemas, and keeping raw immutable lets us rebuild all downstream models
without re-downloading ~190M rows/month. See
docs/decisions/0001-immutable-raw.md.
"""
from __future__ import annotations

import gzip
import io
import json
import logging
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import requests

from pipeline.config.settings import SETTINGS, Settings

logger = logging.getLogger(__name__)

# The stable top-level envelope confirmed against real GH Archive data.
# Everything below `payload` varies by event type, so payload is retained
# as a raw JSON string and parsed downstream in dbt.
_ENVELOPE_COLUMNS = ("id", "type", "actor_login", "repo_name", "payload", "created_at")


@dataclass(frozen=True)
class IngestResult:
    """Outcome of ingesting a single hour."""

    hour_key: str  # e.g. "2024-01-15-15"
    event_count: int
    output_path: Path
    skipped: bool  # True when the partition already existed (idempotent no-op)


def hour_url(dt: datetime, settings: Settings = SETTINGS) -> str:
    """Build the GH Archive URL for a given UTC hour.

    GH Archive uses a non-zero-padded hour, e.g. 2024-01-15-5.json.gz.
    """
    return f"{settings.base_url}/{dt.year:04d}-{dt.month:02d}-{dt.day:02d}-{dt.hour}.json.gz"


def _partition_path(dt: datetime, settings: Settings) -> Path:
    """Hive-style partitioning by date and hour for efficient pruning."""
    return (
        settings.bronze_root
        / f"date={dt.year:04d}-{dt.month:02d}-{dt.day:02d}"
        / f"hour={dt.hour:02d}"
        / "events.parquet"
    )


def download_hour(dt: datetime, settings: Settings = SETTINGS) -> bytes:
    """Download one gzipped hourly file, retrying transient failures.

    Returns the raw gzipped bytes. Integrity is validated by the caller when
    the bytes are decompressed and parsed -- a truncated gzip stream will raise
    there, so we never persist a partial file as if it were complete.
    """
    url = hour_url(dt, settings)
    last_error: Exception | None = None

    for attempt in range(1, settings.max_retries + 1):
        try:
            resp = requests.get(url, timeout=settings.request_timeout_seconds)
            # 404 means the hour genuinely does not exist yet (or ever). That is
            # not retryable and is distinct from a transient failure.
            if resp.status_code == 404:
                raise FileNotFoundError(f"GH Archive hour not available: {url}")
            resp.raise_for_status()
            return resp.content
        except FileNotFoundError:
            raise
        except (requests.RequestException, OSError) as exc:
            last_error = exc
            if attempt == settings.max_retries:
                break
            sleep_s = settings.backoff_base_seconds * (2 ** (attempt - 1))
            logger.warning(
                "download attempt %d/%d for %s failed: %s -- retrying in %.1fs",
                attempt, settings.max_retries, url, exc, sleep_s,
            )
            time.sleep(sleep_s)

    raise RuntimeError(
        f"failed to download {url} after {settings.max_retries} attempts"
    ) from last_error


def parse_events(raw_gzip: bytes) -> list[dict]:
    """Decompress and parse the newline-delimited JSON into envelope rows.

    Decompression here is also the integrity check: a truncated download
    raises on the gzip/JSON boundary rather than silently yielding partial data.
    Malformed individual lines are skipped and counted rather than aborting the
    whole hour -- one bad record should not sink 260k good ones.
    """
    rows: list[dict] = []
    malformed = 0

    with gzip.open(io.BytesIO(raw_gzip), "rt", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                e = json.loads(line)
                rows.append(
                    {
                        "id": e["id"],
                        "type": e["type"],
                        # actor/repo are nested but stable; lift the identifiers
                        # we key on. Full nested objects live inside payload-era
                        # parsing downstream if needed.
                        "actor_login": e.get("actor", {}).get("login"),
                        "repo_name": e.get("repo", {}).get("name"),
                        # payload shape varies by event type -> keep as raw JSON.
                        "payload": json.dumps(e.get("payload", {}), separators=(",", ":")),
                        "created_at": e["created_at"],
                    }
                )
            except (json.JSONDecodeError, KeyError) as exc:
                malformed += 1
                logger.debug("skipping malformed event: %s", exc)

    if malformed:
        logger.warning("skipped %d malformed events during parse", malformed)
    return rows


def _write_parquet_atomic(rows: list[dict], dest: Path) -> None:
    """Write Parquet atomically so a crash mid-write can't leave a partial file.

    We write to a temp path in the same directory, then rename. Rename is atomic
    on POSIX filesystems, so the final partition file is either complete or
    absent -- never half-written. This is what makes reruns safe.
    """
    dest.parent.mkdir(parents=True, exist_ok=True)
    schema = pa.schema(
        [
            ("id", pa.string()),
            ("type", pa.string()),
            ("actor_login", pa.string()),
            ("repo_name", pa.string()),
            ("payload", pa.string()),
            ("created_at", pa.string()),
        ]
    )
    table = pa.Table.from_pylist(rows, schema=schema)
    tmp = dest.with_suffix(dest.suffix + ".tmp")
    pq.write_table(table, tmp, compression="zstd")
    tmp.replace(dest)  # atomic rename


def ingest_hour(
    dt: datetime,
    settings: Settings = SETTINGS,
    overwrite: bool = False,
) -> IngestResult:
    """Ingest a single UTC hour into the bronze layer, idempotently.

    If the partition already exists and `overwrite` is False, this is a no-op
    -- the whole point of the atomic-write + existence-check pattern is that
    reruns and Airflow backfills are safe by default.
    """
    dt = dt.astimezone(UTC).replace(minute=0, second=0, microsecond=0)
    hour_key = f"{dt.year:04d}-{dt.month:02d}-{dt.day:02d}-{dt.hour}"
    dest = _partition_path(dt, settings)

    if dest.exists() and not overwrite:
        logger.info("partition already exists, skipping (idempotent): %s", dest)
        return IngestResult(hour_key, 0, dest, skipped=True)

    logger.info("ingesting hour %s", hour_key)
    raw = download_hour(dt, settings)
    rows = parse_events(raw)
    _write_parquet_atomic(rows, dest)
    logger.info("wrote %d events to %s", len(rows), dest)

    return IngestResult(hour_key, len(rows), dest, skipped=False)
