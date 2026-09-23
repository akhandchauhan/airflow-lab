"""Session 21 · assignment — prove the concurrency funnel throttles.

Boilerplate is ready. Build what the note (§7) asks:
  - a small pool (airflow pools set so_pool 2 "assignment")
  - a task expanded to >=6 sleeping instances, all pool="so_pool"
  - max_active_runs=1 and a max_active_tasks larger than the pool (pool is the bottleneck)
  - one task with a higher priority_weight that starts first when slots are scarce
Then run:
    airflow pools set so_pool 2 "assignment"
    airflow dags test s21_assignment 2026-01-01
"""
from __future__ import annotations

import pendulum
from airflow.sdk import dag, task


@dag(
    dag_id="s21_assignment",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["session-21"],
    default_args={"owner": "akhand", "retries": 1},
)
def pipeline():
    # TODO: expand a sleeping task into a small pool; prove pool (not max_active_tasks) is the cap
    @task
    def todo() -> None:
        print("replace me")

    todo()


pipeline()
