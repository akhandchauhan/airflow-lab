"""Session 27 · assignment — make a slow query fail loud, not hang.

Boilerplate is ready. Build what the note (§9) asks:
  - a cost-capped BigQuery task with execution_timeout and exponential backoff + max_retry_delay
  - a DAG-level dagrun_timeout so a wedged run cannot run forever
  - depends_on_past so a failed day blocks the next day
  - a DeadlineAlert that fires if the run misses its window
Then run:
    airflow dags test s27_assignment 2026-01-01
"""
from __future__ import annotations

import pendulum
from airflow.sdk import dag, task


@dag(
    dag_id="s27_assignment",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["session-27"],
    default_args={"owner": "akhand", "retries": 1},
)
def pipeline():
    # TODO: execution_timeout + exp backoff + max_retry_delay + depends_on_past + dagrun_timeout + DeadlineAlert
    @task
    def todo() -> None:
        print("replace me")

    todo()


pipeline()
