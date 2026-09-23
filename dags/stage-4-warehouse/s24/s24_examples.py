"""Session 24 · examples — custom XCom backends & ObjectStorage.

Boilerplate is ready. Build the note's reference here (§6): fetch a capped BigQuery
scalar, stage it to an ObjectStoragePath, and pass only the path via XCom. Run:
    airflow dags test s24_examples 2026-01-01
"""
from __future__ import annotations

import pendulum
from airflow.sdk import dag, task


@dag(
    dag_id="s24_examples",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["session-24"],
    default_args={"owner": "akhand", "retries": 1},
)
def pipeline():
    # TODO: stage data to an ObjectStoragePath and pass the path (not the payload) via XCom
    @task
    def todo() -> None:
        print("replace me")

    todo()


pipeline()
