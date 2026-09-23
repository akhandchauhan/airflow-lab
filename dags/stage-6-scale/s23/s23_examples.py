"""Session 23 · examples — dbt via Cosmos.

Boilerplate is ready (plain airflow.sdk stub so CI stays green without Cosmos).
Once you `pip install astronomer-cosmos dbt-bigquery` and have a dbt project,
build the note's reference here (§5): a DbtDag over the Stack Overflow dbt
project, one Airflow task per model, connecting via google_cloud_default.
Then run:
    airflow dags test s23_examples 2026-01-01
"""
from __future__ import annotations

import pendulum
from airflow.sdk import dag, task


@dag(
    dag_id="s23_examples",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["session-23"],
    default_args={"owner": "akhand", "retries": 1},
)
def pipeline():
    # TODO: replace with a Cosmos DbtDag once Cosmos + a dbt project are installed
    @task
    def todo() -> None:
        print("replace me")

    todo()


pipeline()
