"""Session 26 · assignment — always tear the resource down.

Boilerplate is ready. Build what the note (§8) asks:
  - a @setup that provisions a scratch BigQuery dataset
  - two work tasks (one that can fail) scoped between setup and teardown
  - a @teardown that drops the dataset and runs even when a work task fails
    (mark it on_failure_fail_dagrun so a broken cleanup fails the run)
Then run:
    airflow dags test s26_assignment 2026-01-01
"""
from __future__ import annotations

import pendulum
from airflow.sdk import dag, task


@dag(
    dag_id="s26_assignment",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["session-26"],
    default_args={"owner": "akhand", "retries": 1},
)
def pipeline():
    # TODO: @setup -> [work_ok, work_flaky] -> @teardown(.as_teardown, on_failure_fail_dagrun=True)
    @task
    def todo() -> None:
        print("replace me")

    todo()


pipeline()
