"""Session 5 · Byte 5.3 — build: a plain DAG that recovers from a flaky task.

Boilerplate is ready. Inside pipeline(), build the two tasks the note (§4) asks for:
  - fetch  : raises ~half the time; retries=3, retry_delay=10s
  - report : runs after fetch, prints a done message
Then run:
    airflow dags test s5_task3 2026-01-01
"""
from __future__ import annotations

import pendulum
from airflow.sdk import dag, task


@dag(
    dag_id="s5_task3",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["session-5"],
    default_args={"owner": "akhand", "retries": 1},
)
def pipeline():
    # TODO byte 5.3: replace this stub with fetch (flaky, retries=3) -> report.
    @task
    def todo() -> None:
        print("replace me")

    todo()


pipeline()
