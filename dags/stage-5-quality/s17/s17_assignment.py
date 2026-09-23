"""Session 17 · assignment — alert on failure.

Boilerplate is ready. Build what the note (§8) asks:
  - a @task computes a Stack Overflow metric via BigQueryHook (capped, no SELECT *)
  - a @task gates it and RAISES when out of band (so failure is reachable)
  - on_failure_callback (a function or a BaseNotifier subclass) alerts with dag/task/exception
  - on_success_callback logs an all-clear; show the SlackNotifier(...) line in a comment
Then run:
    airflow dags test s17_assignment 2026-01-01
"""
from __future__ import annotations

import pendulum
from airflow.sdk import dag, task


@dag(
    dag_id="s17_assignment",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["session-17"],
    default_args={"owner": "akhand", "retries": 1},
)
def pipeline():
    # TODO: alert on failure, clear on success, no secret in the file
    @task
    def todo() -> None:
        print("replace me")

    todo()


pipeline()
