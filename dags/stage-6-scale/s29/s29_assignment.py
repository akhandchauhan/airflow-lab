"""Session 29 · assignment — run a task as a container.

Boilerplate is ready. Build what the note (§8) asks. NOTE: keep this file CI-green —
if the docker / cncf.kubernetes provider is not installed, model the shape with a plain
@task stub and put the real DockerOperator / KubernetesPodOperator only where the
provider exists. Then run:
    airflow dags test s29_assignment 2026-01-01
"""
from __future__ import annotations

import pendulum
from airflow.sdk import dag, task


@dag(
    dag_id="s29_assignment",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["session-29"],
    default_args={"owner": "akhand", "retries": 1},
)
def pipeline():
    # TODO: run a containerised task (stub here for CI; real operator code lives in the note)
    @task
    def todo() -> None:
        print("replace me")

    todo()


pipeline()
