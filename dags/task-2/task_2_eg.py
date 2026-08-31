from __future__ import annotations

import pendulum
from airflow.sdk import DAG, chain
from airflow.providers.standard.operators.empty import EmptyOperator


with DAG(
    dag_id="classic_style_demo",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["course", "demo"],
    default_args={"owner": "akhand", "retries": 2},
) as dag:
    
    start = EmptyOperator(task_id="start")
    job_a = EmptyOperator(task_id="job_a")
    job_b = EmptyOperator(task_id="job_b")
    end = EmptyOperator(task_id="end")

    start >> [job_a, job_b] >> end
