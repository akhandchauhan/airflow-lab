"""Session 30 · assignment — fix a slow-parsing DAG (plain, no BigQuery).

Boilerplate is ready. Build what the note (§8) asks:
  - start from a DAG that does heavy work at top level (sleep/import to simulate a slow parse)
  - measure parse time with `python dags/stage-6-scale/s30/s30_assignment.py`
  - move all import-time work inside task bodies until parse time drops under the target
Then run:
    airflow dags test s30_assignment 2026-01-01
"""
from __future__ import annotations

import pendulum
from airflow.sdk import dag, task


@dag(
    dag_id="s30_assignment",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["session-30"],
    default_args={"owner": "akhand", "retries": 1},
)
def pipeline():
    # TODO: no top-level work; do the heavy import + compute inside the @task
    @task
    def todo() -> None:
        print("replace me")

    todo()


pipeline()
