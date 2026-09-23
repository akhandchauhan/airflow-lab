"""Session 18 · assignment — instrument a plain DAG.

Boilerplate is ready. Build what the note (§8) asks:
  - a @task emits INFO/WARNING/ERROR via the stdlib logging logger, each line meaningful
  - a @task logs a short scannable summary line
  - in the docstring/comments, write the [logging] remote-logging keys (§3) and the
    /api/v2/monitor/health endpoint (§6); plain DAG, no BigQuery, no connection
Then run:
    airflow dags test s18_assignment 2026-01-01
"""
from __future__ import annotations

import pendulum
from airflow.sdk import dag, task


@dag(
    dag_id="s18_assignment",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["session-18"],
    default_args={"owner": "akhand", "retries": 1},
)
def pipeline():
    # TODO: emit well-levelled logs; document remote logging + health endpoint in the docstring
    @task
    def todo() -> None:
        print("replace me")

    todo()


pipeline()
