"""Session 20 · examples — Variables & secrets backends.

Boilerplate is ready. Build the note's reference here (§7): a task that reads a Variable
at run time and feeds it into a cost-capped BigQuery COUNT on posts_questions. Run:
    airflow variables set so_lookback_days 30
    airflow dags test s20_examples 2026-01-01
"""
from __future__ import annotations

import pendulum
from airflow.sdk import dag, task


@dag(
    dag_id="s20_examples",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["session-20"],
    default_args={"owner": "akhand", "retries": 1},
)
def pipeline():
    # TODO: Variable.get inside the task -> capped BigQuery COUNT via BigQueryHook
    @task
    def todo() -> None:
        print("replace me")

    todo()


pipeline()
