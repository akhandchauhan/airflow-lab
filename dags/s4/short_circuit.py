from __future__ import annotations
import pendulum
from airflow.sdk import dag, task


@dag(
    dag_id="s4_short_circuit",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["session-04", "branching"],
    default_args={"owner": "akhand", "retries": 1},
)
def pipeline():
    @task.short_circuit                # ← THE MECHANIC
    def has_new_data() -> bool:
        new_rows = 5
        return new_rows > 0            # return False → EVERYTHING downstream is skipped

    @task
    def load_data():
        print("loading data")

    has_new_data() >> load_data()


pipeline()