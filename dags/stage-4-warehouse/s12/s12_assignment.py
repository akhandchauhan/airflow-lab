"""Session 12 · assignment — the answer-rate layer, driven through hooks.

Boilerplate is ready. Build what the note (§6) asks:
  - a task resolves google_cloud_default via BaseHook.get_connection and logs
    conn_type + project (proves no secret lives in the file)
  - a task computes the answer rate with BigQueryHook.get_first on posts_questions
  - a task pulls a small multi-row result with BigQueryHook.get_records
  - a final task logs a one-line summary from the values above
All external access via hooks, built INSIDE tasks; no hardcoded creds. Then run:
    airflow dags test s12_assignment 2026-01-01
"""
from __future__ import annotations

import pendulum
from airflow.sdk import dag, task


@dag(
    dag_id="s12_assignment",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["session-12"],
    default_args={"owner": "akhand", "retries": 1},
)
def pipeline():
    # TODO: resolve the Connection with BaseHook.get_connection, then use
    # BigQueryHook.get_first / get_records to build the answer-rate layer.
    @task
    def todo() -> None:
        print("replace me")

    todo()


pipeline()
