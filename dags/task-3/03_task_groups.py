from __future__ import annotations

import pendulum
from airflow.sdk import dag, task_group, task


@dag(
    dag_id="03_task_groups",
    start_date=pendulum.datetime(2026, 9, 1, tz='UTC'),
    catchup=False,
    schedule=None,
    tags=['task_group', 'override'],
    default_args={"owner": "akhand", "retries": 2},
)

def pipeline():

    @task_group(group_id='src')
    def load_source(name: str) -> None:

        @task
        def download(src: str) -> str:
            return f"gs://{src}"

        @task_group(group_id='quality')
        def run_checks(path: str) -> str:

            @task
            def check_nulls(path: str) -> str:
                return f"No nulls found in this {path} path"

            @task
            def check_schema(checked:str) -> str:
                return "everything okay"

            return check_schema(check_nulls(path))

        @task
        def stage(path: str) -> None:
            print(f"staged {path}")

        stage(run_checks(download(name)))


    for name in ['orders', 'users', 'products']:
        load_source.override(group_id = f"src_{name}")(name)


pipeline()