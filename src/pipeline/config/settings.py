"""Central configuration for the GitHub Events pipeline.

Scope decision (see docs/decisions/0002-event-type-scope.md): we model four
high-value event types in depth and retain the rest as raw envelope only.
These four account for ~80% of daily event volume.
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

# Event types we transform in depth downstream. All events are still landed
# in bronze; this list drives the silver/gold modeling scope only.
MODELED_EVENT_TYPES: tuple[str, ...] = (
    "PushEvent",
    "PullRequestEvent",
    "IssuesEvent",
    "WatchEvent",
)

# GH Archive publishes one gzipped JSON file per hour at this base URL.
GHARCHIVE_BASE_URL = "https://data.gharchive.org"


@dataclass(frozen=True)
class Settings:
    """Runtime settings, overridable via environment variables."""

    # Root for the bronze (raw) partitioned Parquet layer.
    data_root: Path = field(
        default_factory=lambda: Path(os.getenv("PIPELINE_DATA_ROOT", "data"))
    )
    base_url: str = os.getenv("GHARCHIVE_BASE_URL", GHARCHIVE_BASE_URL)

    # Network retry policy for the file download.
    max_retries: int = int(os.getenv("INGEST_MAX_RETRIES", "5"))
    backoff_base_seconds: float = float(os.getenv("INGEST_BACKOFF_BASE", "2.0"))
    request_timeout_seconds: int = int(os.getenv("INGEST_TIMEOUT", "60"))

    @property
    def bronze_root(self) -> Path:
        return self.data_root / "bronze" / "github_events"


SETTINGS = Settings()
