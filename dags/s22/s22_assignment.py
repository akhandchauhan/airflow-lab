"""Session 22 · assignment — wire the CI gate and pass every integrity check.

Boilerplate is ready. Build what the note (§6) asks:
  - a plain DAG (no BigQuery) that passes tags/owner/retries and survives AIR3
  - add .github/workflows/ci.yml running ruff --select AIR3 + pytest on push
  - make that workflow a required status check so a red run blocks the merge
Then run:
    airflow dags test s22_assignment 2026-01-01
"""
from __future__ import annotations

import pendulum
from airflow.sdk import dag, task


@dag(
    dag_id="s22_assignment",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["session-22"],
    default_args={"owner": "akhand", "retries": 1},
)
def pipeline():
    # TODO: build a plain DAG that passes the integrity gates; wire CI to run them
    @task
    def todo() -> None:
        print("replace me")

    todo()


pipeline()
