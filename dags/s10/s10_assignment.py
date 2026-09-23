"""Session 10 · assignment — a backfill-safe, reprocessable DAG."""
from __future__ import annotations

import pendulum
from airflow.sdk import dag, task


@dag(
    dag_id="s10_assignment",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["session-10"],
    default_args={"owner": "akhand", "retries": 1},
)
def pipeline():
    # TODO: build what the note (§7) asks — a task whose output derives ONLY from
    # the data interval (idempotent), safe to re-run under --reprocess-behavior completed.
    @task
    def todo() -> None:
        print("replace me")

    todo()


pipeline()
