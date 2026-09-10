"""Session 7 · assignment — a param-driven DAG.

params: country (string), limit (integer, minimum=1), env (enum dev/prod).
One task reads all three and prints the report line. Run:
    airflow dags test s7_task3 2026-01-01
    airflow dags test s7_task3 2026-01-01 --conf '{"country": "US", "limit": 5, "env": "prod"}'
"""
from __future__ import annotations

import pendulum
from airflow.sdk import dag, task, Param, get_current_context


@dag(
    dag_id="s7_task3",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["session-7"],
    params={
        "country": Param("IN", type="string"),
        "limit": Param(10, type="integer", minimum=1),
        "env": Param("dev", type="string", enum=["dev", "prod"]),
    },
    default_args={"owner": "akhand", "retries": 1},
)
def pipeline():

    @task(retries=2)
    def report() -> None:
        inputs = get_current_context()["params"]
        print(f"report for {inputs['country']}, top {inputs['limit']}, env={inputs['env']}")

    report()


pipeline()
