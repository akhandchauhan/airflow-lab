"""Session 6 · Byte 6.1 — declare and read a param.

Boilerplate is ready. Add a params={...} dict to @dag and read it in the task with
get_current_context()["params"] (see the note, §1–§3), then run:
    airflow dags test s6_task1 2026-01-01
    airflow dags test s6_task1 2026-01-01 --conf '{"name": "Panda"}'
"""
from __future__ import annotations

import pendulum
from airflow.sdk import dag, task


@dag(
    dag_id="s6_task1",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["session-6"],
    default_args={"owner": "akhand", "retries": 1},
)
def pipeline():
    # TODO byte 6.1: add params= to @dag above; read them here via get_current_context().
    @task
    def todo() -> None:
        print("replace me")

    todo()


pipeline()
