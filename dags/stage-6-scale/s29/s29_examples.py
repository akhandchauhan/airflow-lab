"""Session 29 · examples — container tasks (DockerOperator / KubernetesPodOperator).

Boilerplate is ready. Build the note's reference here (§7). NOTE: the docker and
cncf.kubernetes providers may not be installed in CI, so keep THIS file plain
airflow.sdk stubs — copy the real DockerOperator / KubernetesPodOperator code from the
note into a scratch DAG only where those providers are installed. Run:
    airflow dags test s29_examples 2026-01-01
"""
from __future__ import annotations

import pendulum
from airflow.sdk import dag, task


@dag(
    dag_id="s29_examples",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["session-29"],
    default_args={"owner": "akhand", "retries": 1},
)
def pipeline():
    # TODO: mirror the note's container task shape (stub only here; real operator lives in the note)
    @task
    def todo() -> None:
        print("replace me")

    todo()


pipeline()
