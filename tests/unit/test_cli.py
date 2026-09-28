"""Unit tests for the ingestion CLI."""
from __future__ import annotations

import pytest

from pipeline.ingest import cli
from pipeline.ingest.gharchive import IngestResult

# ---- hour spec parsing -------------------------------------------------------

def test_parse_hours_single():
    assert cli._parse_hours("15") == [15]


def test_parse_hours_range_inclusive():
    assert cli._parse_hours("9-11") == [9, 10, 11]


def test_parse_hours_full_day_default():
    assert cli._parse_hours("0-23") == list(range(24))


@pytest.mark.parametrize("bad", ["24", "-1", "5-30", "17-9"])
def test_parse_hours_rejects_invalid(bad):
    with pytest.raises(ValueError):
        cli._parse_hours(bad)


# ---- main() orchestration ----------------------------------------------------

def test_main_rejects_bad_date():
    assert cli.main(["--date", "2024/01/15"]) == 2


def test_main_rejects_bad_hours():
    assert cli.main(["--date", "2024-01-15", "--hours", "99"]) == 2


def test_main_ingests_requested_hours(monkeypatch):
    calls = []

    def fake_ingest(dt, settings, overwrite=False):
        calls.append(dt.hour)
        return IngestResult(f"k-{dt.hour}", 100, None, skipped=False)

    monkeypatch.setattr(cli, "ingest_hour", fake_ingest)
    rc = cli.main(["--date", "2024-01-15", "--hours", "9-11"])
    assert rc == 0
    assert calls == [9, 10, 11]


def test_main_returns_nonzero_when_an_hour_fails(monkeypatch):
    def boom(dt, settings, overwrite=False):
        raise RuntimeError("network down")

    monkeypatch.setattr(cli, "ingest_hour", boom)
    rc = cli.main(["--date", "2024-01-15", "--hours", "5"])
    assert rc == 1


def test_main_treats_missing_hour_as_skip_not_failure(monkeypatch):
    def not_published(dt, settings, overwrite=False):
        raise FileNotFoundError("not available")

    monkeypatch.setattr(cli, "ingest_hour", not_published)
    # A not-yet-published hour is a skip, not a failure -> exit 0.
    rc = cli.main(["--date", "2024-01-15", "--hours", "5"])
    assert rc == 0
