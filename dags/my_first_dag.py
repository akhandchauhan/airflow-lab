from __future__ import annotations

import pendulum
from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator

dag = DAG(
    dag_id="my_first_dag",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["lecture-4"],
    default_args={"owner": "akhand", "retries": 2},
)


def print_context(**context):
    print(context)
    print("Job Completed")


copy_file = BashOperator(
    dag=dag,
    task_id="copy_file",
    bash_command="echo copying file",
)

task2 = PythonOperator(
    task_id="task2",
    python_callable=print_context,
    dag=dag,
)

copy_file >> task2
