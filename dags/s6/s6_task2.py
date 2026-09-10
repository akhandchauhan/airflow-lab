"""Session 6 · Byte 6.6 — build: a small cost-capped BigQuery DAG.

Boilerplate is ready. Build what the note (§6) asks:
  - a capped BigQueryInsertJobOperator aggregate
  - a BigQueryCheckOperator that fails if a metric is empty
  - a @task using BigQueryHook.get_first to log one scalar
Then run (needs the google_cloud_default connection):
    airflow dags test s6_task2 2026-01-01
"""
from __future__ import annotations

import pendulum
from airflow.sdk import dag, task


@dag(
    dag_id="s6_task2",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["session-6"],
    default_args={"owner": "akhand", "retries": 1},
)
def pipeline():
    # TODO byte 6.6: replace this stub with your capped BigQuery job + check + hook read.
    @task
    def todo() -> None:
        print("replace me")

    todo()


pipeline()
