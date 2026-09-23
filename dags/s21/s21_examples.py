"""Session 21 · examples — executors & concurrency.

Boilerplate is ready. Build the note's reference here (§6): a DAG with max_active_runs=1,
max_active_tasks, a pool, and priority_weight, expanding a sleeping task to watch queuing.
First create the pool, then run:
    airflow pools set demo_pool 2 "session 21 demo"
    airflow dags test s21_examples 2026-01-01
"""
from __future__ import annotations

import pendulum
from airflow.sdk import dag, task


@dag(
    dag_id="s21_examples",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["session-21"],
    default_args={"owner": "akhand", "retries": 1},
)
def pipeline():
    # TODO: add max_active_runs/max_active_tasks + pool + priority_weight; expand a sleeping task
    @task
    def todo() -> None:
        print("replace me")

    todo()


pipeline()
