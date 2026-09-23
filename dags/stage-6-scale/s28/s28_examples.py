"""Session 28 · examples — dependency isolation (venv / external python).

Boilerplate is ready. Build the note's reference here (§7): a @task.virtualenv that
installs a pinned dep in a per-run venv, plus a @task.external_python pointing at a
prebuilt interpreter, then a task that consumes the returned scalar. Run:
    airflow dags test s28_examples 2026-01-01
"""
from __future__ import annotations

import pendulum
from airflow.sdk import dag, task


@dag(
    dag_id="s28_examples",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["session-28"],
    default_args={"owner": "akhand", "retries": 1},
)
def pipeline():
    # TODO: add a @task.virtualenv (pinned requirements) + a @task.external_python, then consume the result
    @task
    def todo() -> None:
        print("replace me")

    todo()


pipeline()
