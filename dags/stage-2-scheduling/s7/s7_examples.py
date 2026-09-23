"""Session 07 · examples — native backfill & idempotency."""
from __future__ import annotations

import pendulum
from airflow.sdk import dag, task


@dag(
    dag_id="s7_examples",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["session-7"],
    default_args={"owner": "akhand", "retries": 1},
)
def pipeline():
    # TODO: build the note's reference (§6) — a @daily task that prints its
    # data_interval window, then backfill a week with `airflow backfill create`.
    @task
    def todo() -> None:
        print("replace me")

    todo()


pipeline()
