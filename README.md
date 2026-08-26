# GitHub Events ELT

Batch ELT pipeline over [GH Archive](https://www.gharchive.org/) — the public
stream of every GitHub event (~6.4M events/day). Ingests raw events into a
partitioned bronze layer, then transforms them into an analytics-ready
dimensional model.

> **Status:** early development. Ingestion layer complete and tested;
> transformation and orchestration in progress.

## Why this project

GH Archive is deliberately messy: 15 event types, each with a different
payload schema, plus optional fields and nested arrays. It's real volume
(~190M events/month) — enough that full reloads aren't viable and incremental,
idempotent processing is a requirement, not a nice-to-have.

## Architecture

Raw GH Archive files → **bronze** (partitioned Parquet, immutable) →
**silver** (dbt: per-event-type parsing, dedupe) →
**gold** (dbt: dimensional model) → analytics.

Stack: Python · Airflow · dbt · DuckDB · Parquet.

## What's built

- **Ingestion** (`src/pipeline/ingest/`) — idempotent, backfill-safe download
  and landing into partitioned Parquet, with retry/backoff and integrity
  checks. Rerunning any hour is a safe no-op.

## Planned

- Airflow DAG with catchup-based backfill
- dbt models: staging (per event type) → intermediate → dimensional marts
- Data quality tests encoding real business rules
- Docker Compose one-command local setup

## Development

```bash
pip install -e ".[dev]"
pytest          # run tests
ruff check src  # lint
mypy src        # type check
```

## Design decisions

See [`docs/decisions/`](docs/decisions/) for architecture decision records.
