"""Session 22 · examples — DAG versioning & CI/CD.

Boilerplate is ready. Build the note's reference here (§5): a plain DAG you can
change structurally (add a task / rename a task_id) to mint a new DAG version,
then compare versions in the grid. Then run:
    airflow dags test s22_examples 2026-01-01
"""
from __future__ import annotations

import pendulum
from airflow.sdk import dag, task


@dag(
    dag_id="s22_examples",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["session-22"],
    default_args={"owner": "akhand", "retries": 1},
)
def pipeline():
    # TODO: build the versioning demo — change its structure to mint a new version
    @task
    def todo() -> None:
        print("replace me")

    todo()


pipeline()
