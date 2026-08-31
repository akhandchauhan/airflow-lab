"""Session 01 - TaskFlow foundations. See docs/course/01-taskflow-foundations.md."""

from __future__ import annotations

import pendulum
from airflow.sdk import dag, task


@dag(
    dag_id="task_1_foundations_example",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["course", "w1", "taskflow"],
    default_args={"owner": "akhand", "retries": 2},
)
def pipeline():
    @task(multiple_outputs=True)
    def extract() -> dict:
        return {"path": "gs://lake/raw/2026-01-01", "rows": 4213}

    @task
    def transform(path: str, rows: int) -> int:
        print(f"transforming {rows} rows from {path}")
        return rows

    @task
    def load(row_count: int) -> None:
        print(f"loaded {row_count} rows")

    data = extract()
    load(transform(path=data["path"], rows=data["rows"]))


pipeline()
