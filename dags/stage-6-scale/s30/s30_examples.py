"""Session 30 · examples — parsing performance (plain DAG, no BigQuery).

Boilerplate is ready. Build the note's reference here (§7): move an expensive import and a
config read OFF the top level and INTO a @task, then time the parse with
`python dags/stage-6-scale/s30/s30_examples.py`. Run:
    airflow dags test s30_examples 2026-01-01
"""
from __future__ import annotations

import pendulum
from airflow.sdk import dag, task


@dag(
    dag_id="s30_examples",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["session-30"],
    default_args={"owner": "akhand", "retries": 1},
)
def pipeline():
    # TODO: keep top level cheap; put heavy import + I/O inside the @task body
    @task
    def todo() -> None:
        print("replace me")

    todo()


pipeline()
