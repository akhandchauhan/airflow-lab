"""Session 6 · Byte 6.3 — build: a param-driven DAG.

Boilerplate is ready. Build what the note (§4) asks:
  - params: country (string, default "IN"), limit (integer, minimum=1, default 10)
  - one task that reads both and prints e.g. "report for IN, top 10"
Then run:
    airflow dags test s6_task3 2026-01-01
    airflow dags test s6_task3 2026-01-01 --conf '{"country": "US", "limit": 5}'
"""
from __future__ import annotations

import pendulum
from airflow.sdk import dag, task


@dag(
    dag_id="s6_task3",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["session-6"],
    default_args={"owner": "akhand", "retries": 1},
)
def pipeline():
    # TODO byte 6.3: add the two params above; read both; print the report line.
    @task
    def todo() -> None:
        print("replace me")

    todo()


pipeline()
