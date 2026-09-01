from __future__ import annotations

import pendulum
from airflow.sdk import dag, task, task_group


@dag(
    dag_id="task_group_demo",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["course", "demo"],
    default_args={"owner": "akhand", "retries": 2},
)
def pipeline():

    @task_group(group_id="ingest")
    def ingest(name: str) -> None:
        @task
        def download(src: str) -> str:         # -> ingest_orders.download
            return f"/raw/{src}"

        @task
        def validate(path: str) -> str:        # -> ingest_orders.validate
            return path

        @task
        def stage(path: str) -> None:          # -> ingest_orders.stage
            print(f"staging {path}")

        # TaskFlow wiring inside the group
        stage(validate(download(name)))

    # instantiate the same group per source, distinct group_id each time
    for name in ["orders", "users"]:
        ingest.override(group_id=f"ingest_{name}")(name)


pipeline()
