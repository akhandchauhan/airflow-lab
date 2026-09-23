"""Session 24 · assignment — move a result set out of the metadata DB.

Boilerplate is ready. Build what the note (§7) asks:
  - a capped BigQuery query returning a real result set (e.g. top 20 tags)
  - write those rows to an ObjectStoragePath, return only the path
  - a downstream task reads the object back and logs a derived figure
Then run:
    airflow dags test s24_assignment 2026-01-01
"""
from __future__ import annotations

import pendulum
from airflow.sdk import dag, task


@dag(
    dag_id="s24_assignment",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["session-24"],
    default_args={"owner": "akhand", "retries": 1},
)
def pipeline():
    # TODO: stage the rows to object storage; only an ObjectStoragePath crosses XCom
    @task
    def todo() -> None:
        print("replace me")

    todo()


pipeline()
