# 1. Keep the raw layer immutable

Date: 2026-08-26

## Status

Accepted

## Context

GH Archive publishes ~6.4M events/day (~190M/month) across 15 event types,
each with a different payload schema. We need to turn these raw files into an
analytics-ready model, and that modeling logic will change over time as we
discover edge cases, add event types, or fix bugs.

Two broad approaches:

- **ETL** — transform on the way in, store only the cleaned result.
- **ELT** — store the raw source untouched, transform downstream.

## Decision

We land the raw event envelope in a bronze layer as partitioned Parquet and
never modify it. All cleaning, parsing, and reshaping happens in downstream
(silver/gold) models that read from bronze. The `payload` field is kept as a
raw JSON string rather than being flattened at ingest time.

## Consequences

**Positive**

- Transformation logic can be rewritten and the entire history rebuilt from
  bronze without re-downloading ~190M rows/month from GH Archive.
- A bug in a transformation never corrupts the source of truth — we always
  have the original to recover from.
- Keeping `payload` as raw JSON means new event types or newly-relevant
  payload fields require no re-ingest — only a new downstream model.

**Negative**

- Storage cost is higher than storing only cleaned data. Measured on a real
  hour: bronze Parquet (zstd) is ~105MB vs ~136MB source gzip — only ~1.3x
  compression, because the JSON payload strings dominate the bytes and JSON
  in a string column does not compress as well as native columnar types.
- Consumers must parse `payload` JSON themselves; bronze is not directly
  friendly for ad-hoc queries.

The storage cost is an accepted trade-off: re-downloadability and a
recoverable source of truth are worth more than the storage saved, especially
at this data's price point.
