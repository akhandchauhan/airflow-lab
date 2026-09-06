from __future__ import annotations
import pendulum
from airflow.sdk import dag, task

# ROWS = 500


@dag(
    dag_id="s04_branch_example_exercise",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["session-04", "branching"],
    default_args={"owner": "akhand", "retries": 1},
)
def pipeline():

    @task
    def rows_count() -> int:
        rows = 3000
        return rows

    @task.branch
    def check_count(rows: int) -> str:
        # returns the TASK_ID string of the path to run; the other is skipped
        return "incremental_load" if rows > 500 else "full_truncate_reload"

    @task(task_id="incremental_load")
    def run_incremental_load():
        print("doing incremental load of rows")

    @task(task_id="full_truncate_reload")
    def run_full_reload():
        print("will first truncate the whole table, then reload with full data")

    path = check_count(rows_count())
    path >> [run_incremental_load(), run_full_reload()]


pipeline()
