"""Command-line entrypoint for ingesting GH Archive hours.

Examples
--------
    # one full day
    python -m pipeline.ingest.cli --date 2024-01-15

    # a specific hour range (inclusive)
    python -m pipeline.ingest.cli --date 2024-01-15 --hours 9-17

    # force re-ingest even if partitions already exist
    python -m pipeline.ingest.cli --date 2024-01-15 --overwrite
"""
from __future__ import annotations

import argparse
import logging
import sys
from datetime import UTC, datetime

from pipeline.config.settings import SETTINGS
from pipeline.ingest.gharchive import ingest_hour

logger = logging.getLogger("pipeline.ingest")


def _parse_hours(spec: str) -> list[int]:
    """Parse an hours spec into a sorted list of ints.

    Accepts a single hour ("15"), an inclusive range ("9-17"), or the default
    full day ("0-23"). Rejects anything outside 0..23.
    """
    spec = spec.strip()
    if "-" in spec:
        start_s, end_s = spec.split("-", 1)
        start, end = int(start_s), int(end_s)
    else:
        start = end = int(spec)

    if not (0 <= start <= 23 and 0 <= end <= 23):
        raise ValueError("hours must be within 0-23")
    if start > end:
        raise ValueError(f"invalid hour range: {spec} (start after end)")
    return list(range(start, end + 1))


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="ingest",
        description="Ingest GH Archive hourly event files into the bronze layer.",
    )
    p.add_argument(
        "--date",
        required=True,
        help="UTC date to ingest, formatted YYYY-MM-DD.",
    )
    p.add_argument(
        "--hours",
        default="0-23",
        help="Hour or inclusive range within the day (default: 0-23).",
    )
    p.add_argument(
        "--overwrite",
        action="store_true",
        help="Re-ingest hours even if their partition already exists.",
    )
    return p


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
    )
    args = _build_parser().parse_args(argv)

    try:
        day = datetime.strptime(args.date, "%Y-%m-%d").replace(tzinfo=UTC)
    except ValueError:
        logger.error("invalid --date %r, expected YYYY-MM-DD", args.date)
        return 2

    try:
        hours = _parse_hours(args.hours)
    except ValueError as exc:
        logger.error("invalid --hours %r: %s", args.hours, exc)
        return 2

    ingested = skipped = failed = total_events = 0

    for hour in hours:
        dt = day.replace(hour=hour)
        try:
            result = ingest_hour(dt, SETTINGS, overwrite=args.overwrite)
            if result.skipped:
                skipped += 1
            else:
                ingested += 1
                total_events += result.event_count
        except FileNotFoundError:
            # Hour not published (e.g. ingesting "today" ahead of the feed).
            logger.warning("hour %s not available yet, skipping", f"{args.date}-{hour}")
            skipped += 1
        except Exception:
            failed += 1
            logger.exception("failed to ingest hour %s", f"{args.date}-{hour}")

    logger.info(
        "done: %d ingested (%d events), %d skipped, %d failed",
        ingested, total_events, skipped, failed,
    )
    # Non-zero exit if any hour failed, so schedulers/CI can detect it.
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
