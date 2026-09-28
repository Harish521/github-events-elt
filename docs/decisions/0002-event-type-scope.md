# 2. Model four event types in depth, retain the rest as raw

Date: 2026-08-26

## Status

Accepted

## Context

GH Archive emits 15 event types. Their volume is highly skewed. Measured
across one real hour (266,871 events):

| Event type              | Share  |
|-------------------------|--------|
| PushEvent               | ~48%   |
| CreateEvent             | ~8%    |
| PullRequestEvent        | ~5%    |
| IssueCommentEvent       | ~3%    |
| WatchEvent              | ~3%    |
| IssuesEvent             | ~1%    |
| ...9 more               | tail   |

Each type has its own payload schema, so modeling a type in depth means
writing and testing dedicated parsing and dimensional logic. Modeling all 15
is a large surface area with diminishing analytical value in the long tail.

## Decision

For this project we model four event types in depth:

- **PushEvent** — code activity, the dominant signal
- **PullRequestEvent** — collaboration / review activity
- **IssuesEvent** — project management activity
- **WatchEvent** — popularity / interest signal (a "star")

All other event types are still landed in bronze in full, but are not parsed
into silver/gold models. Nothing is discarded — the raw payload for every
event type remains queryable in bronze.

## Consequences

**Positive**

- The four chosen types cover the highest-value analytical questions (code
  velocity, collaboration, popularity) while keeping the modeling surface
  small enough to build, test, and document well.
- Because bronze retains everything, extending to more event types later is
  purely additive — write a new silver model, no re-ingest.

**Negative**

- Analytics on unmodeled event types require querying raw JSON in bronze
  directly, which is less convenient.

This is a deliberate scope choice to reach a polished, well-tested state
rather than a broad, shallow one. The immutable-raw design (ADR 0001) makes
the choice cheap to revisit.
