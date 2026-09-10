"""Session 7 · Byte 6.3 — build: a param-driven DAG.

Boilerplate is ready. Build what the note (§4) asks:
  - params: country (string, default "IN"), limit (integer, minimum=1, default 10)
  - one task that reads both and prints e.g. "report for IN, top 10"
Then run:
    airflow dags test s7_task3 2026-01-01
    airflow dags test s7_task3 2026-01-01 --conf '{"country": "US", "limit": 5}'
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
    params = {
        'country':Param('IN', type ='string'),
        'limit':Param(10,type='integer', minimum =1),
        'env':Param('dev', type ="string", enum =['dev','prod']),
    },
    default_args={"owner": "akhand", "retries": 1},
)
def pipeline():
    # TODO byte 6.3: add the two params above; read both; print the report line.
    @task(retries = 2)
    def todo() -> None:
        inputs = get_current_context()['params']
        print(f"report for {inputs['country']}, top {inputs['limit']}, {inputs['env']}")
    todo()


pipeline()
