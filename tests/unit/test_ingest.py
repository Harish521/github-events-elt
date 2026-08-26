"""Unit tests for the GH Archive ingestion module.

These target the two claims the README makes about this pipeline:
  * reruns are idempotent (safe backfill), and
  * one malformed record does not sink the whole hour.
Plus the supporting resilience behaviours (retry/backoff, 404, atomicity).
"""
from __future__ import annotations

import gzip
import io
import json
from datetime import UTC, datetime

import pytest

from pipeline.config.settings import Settings
from pipeline.ingest import gharchive


def _make_gzip(events: list[dict]) -> bytes:
    """Build a gzipped newline-delimited JSON blob like a real GH Archive file."""
    buf = io.BytesIO()
    with gzip.open(buf, "wt", encoding="utf-8") as fh:
        for e in events:
            fh.write(json.dumps(e) + "\n")
    return buf.getvalue()


def _event(event_id: str, event_type: str = "PushEvent") -> dict:
    return {
        "id": event_id,
        "type": event_type,
        "actor": {"login": "octocat"},
        "repo": {"name": "octocat/hello-world"},
        "payload": {"push_id": 123, "size": 1},
        "created_at": "2024-01-15T15:00:00Z",
    }


@pytest.fixture
def settings(tmp_path) -> Settings:
    return Settings(data_root=tmp_path, max_retries=3, backoff_base_seconds=0.0)


DT = datetime(2024, 1, 15, 15, tzinfo=UTC)


# ---- URL construction --------------------------------------------------------

def test_hour_url_does_not_zero_pad_hour(settings):
    # GH Archive uses a non-padded hour: 2024-01-15-5.json.gz, not -05.
    url = gharchive.hour_url(datetime(2024, 1, 15, 5, tzinfo=UTC), settings)
    assert url.endswith("/2024-01-15-5.json.gz")


# ---- parsing / data quality --------------------------------------------------

def test_parse_events_extracts_envelope():
    raw = _make_gzip([_event("1"), _event("2", "WatchEvent")])
    rows = gharchive.parse_events(raw)
    assert [r["id"] for r in rows] == ["1", "2"]
    assert rows[0]["actor_login"] == "octocat"
    assert rows[0]["repo_name"] == "octocat/hello-world"
    # payload retained as raw JSON string, not flattened
    assert json.loads(rows[0]["payload"])["push_id"] == 123


def test_parse_events_skips_malformed_without_aborting():
    good = json.dumps(_event("1"))
    bad = "{not valid json"
    also_good = json.dumps(_event("2"))
    buf = io.BytesIO()
    with gzip.open(buf, "wt", encoding="utf-8") as fh:
        fh.write(good + "\n" + bad + "\n" + also_good + "\n")
    rows = gharchive.parse_events(buf.getvalue())
    # one bad record dropped, two good records survive
    assert [r["id"] for r in rows] == ["1", "2"]


def test_parse_events_handles_empty_payload():
    # PublicEvent has an empty payload in real data -- must not crash.
    ev = _event("1", "PublicEvent")
    ev["payload"] = {}
    rows = gharchive.parse_events(_make_gzip([ev]))
    assert json.loads(rows[0]["payload"]) == {}


# ---- idempotency (the headline behaviour) ------------------------------------

def test_ingest_hour_is_idempotent(settings, monkeypatch):
    raw = _make_gzip([_event("1"), _event("2")])
    calls = {"n": 0}

    def fake_download(dt, s):
        calls["n"] += 1
        return raw

    monkeypatch.setattr(gharchive, "download_hour", fake_download)

    first = gharchive.ingest_hour(DT, settings)
    assert first.event_count == 2 and not first.skipped

    # second run of the same hour is a no-op and does not re-download
    second = gharchive.ingest_hour(DT, settings)
    assert second.skipped is True
    assert calls["n"] == 1  # download not called again


def test_ingest_hour_overwrite_forces_rewrite(settings, monkeypatch):
    monkeypatch.setattr(gharchive, "download_hour", lambda dt, s: _make_gzip([_event("1")]))
    gharchive.ingest_hour(DT, settings)
    result = gharchive.ingest_hour(DT, settings, overwrite=True)
    assert result.skipped is False


# ---- resilience --------------------------------------------------------------

def test_download_retries_then_succeeds(settings, monkeypatch):
    import requests

    attempts = {"n": 0}

    class Resp:
        status_code = 200
        content = b"ok"

        def raise_for_status(self):
            pass

    def flaky_get(url, timeout):
        attempts["n"] += 1
        if attempts["n"] < 3:
            raise requests.ConnectionError("boom")
        return Resp()

    monkeypatch.setattr(requests, "get", flaky_get)
    assert gharchive.download_hour(DT, settings) == b"ok"
    assert attempts["n"] == 3


def test_download_404_is_not_retried(settings, monkeypatch):
    import requests

    attempts = {"n": 0}

    class Resp404:
        status_code = 404

        def raise_for_status(self):
            pass

    def get_404(url, timeout):
        attempts["n"] += 1
        return Resp404()

    monkeypatch.setattr(requests, "get", get_404)
    with pytest.raises(FileNotFoundError):
        gharchive.download_hour(DT, settings)
    assert attempts["n"] == 1  # not retried


def test_write_is_atomic_no_tmp_left_behind(settings, monkeypatch):
    monkeypatch.setattr(gharchive, "download_hour", lambda dt, s: _make_gzip([_event("1")]))
    result = gharchive.ingest_hour(DT, settings)
    assert result.output_path.exists()
    # no leftover .tmp file
    tmp_files = list(result.output_path.parent.glob("*.tmp"))
    assert tmp_files == []
