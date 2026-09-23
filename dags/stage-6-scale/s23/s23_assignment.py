"""Session 23 · assignment — embed dbt as one stage via DbtTaskGroup.

Boilerplate is ready (plain airflow.sdk stub so CI stays green without Cosmos).
Build what the note (§6) asks, once Cosmos + a dbt project are installed:
  - a DAG shaped extract -> DbtTaskGroup -> publish
  - the dbt group connects to BigQuery via a ProfileConfig from google_cloud_default
  - RenderConfig(select=...) renders only one layer; models are cost-capped, no SELECT *
Then run:
    airflow dags test s23_assignment 2026-01-01
"""
from __future__ import annotations

import pendulum
from airflow.sdk import dag, task


@dag(
    dag_id="s23_assignment",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["session-23"],
    default_args={"owner": "akhand", "retries": 1},
)
def pipeline():
    # TODO: extract() >> DbtTaskGroup(...) >> publish() once Cosmos is installed
    @task
    def todo() -> None:
        print("replace me")

    todo()


pipeline()
