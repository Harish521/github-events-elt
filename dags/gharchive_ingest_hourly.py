"""Airflow DAG: hourly ingestion of GH Archive events into the bronze layer.

Scheduling model
----------------
This DAG runs once per hour. Each DAG run is responsible for exactly one
GH Archive hour -- the hour that just completed. We rely on Airflow's data
interval (``logical_date``) rather than wall-clock "now", so that:

  * **Backfill works.** With ``catchup=True`` and a past ``start_date``,
    Airflow schedules one run per historical hour and replays them in order.
    This is how we load history.
  * **Reruns are safe.** Ingestion is idempotent (atomic write + existence
    check), so Airflow retries and manual re-runs never duplicate data.

GH Archive publishes an hour's file shortly after the hour closes, so we
offset by looking at the *data interval start* of each run.
"""
from __future__ import annotations

import pendulum
from airflow.decorators import dag, task

# The pipeline package is installed into the Airflow image (see the compose
# setup); this import resolves at DAG-parse time on the worker.
from pipeline.config.settings import SETTINGS
from pipeline.ingest.gharchive import ingest_hour


@dag(
    dag_id="gharchive_ingest_hourly",
    description="Ingest one GH Archive hour per run into the bronze Parquet layer.",
    schedule="@hourly",
    # Start well in the past so an initial deploy backfills recent history.
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=True,
    # Cap concurrent backfill runs so we don't hammer GH Archive or the disk.
    max_active_runs=3,
    default_args={
        "retries": 3,
        "retry_delay": pendulum.duration(minutes=2),
        "retry_exponential_backoff": True,
        "max_retry_delay": pendulum.duration(minutes=15),
    },
    tags=["gharchive", "ingestion", "bronze"],
)
def gharchive_ingest_hourly():
    @task
    def ingest(data_interval_start: pendulum.DateTime | None = None) -> dict:
        """Ingest the single hour this run is responsible for.

        ``data_interval_start`` is injected by Airflow and is the start of the
        hour this run covers -- deterministic and independent of when the task
        actually executes, which is what makes backfill and retries correct.
        """
        assert data_interval_start is not None  # always provided at runtime
        # data_interval_start is already tz-aware UTC. ingest_hour normalizes
        # to UTC and truncates to the hour internally, so we pass it straight
        # through -- no manual conversion needed.
        result = ingest_hour(data_interval_start, SETTINGS)

        return {
            "hour": result.hour_key,
            "events": result.event_count,
            "skipped": result.skipped,
        }

    ingest()


dag_instance = gharchive_ingest_hourly()
