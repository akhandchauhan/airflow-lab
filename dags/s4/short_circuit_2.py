from __future__ import annotations
import pendulum
from airflow.sdk import dag, task


@dag(
    dag_id='s4_short_circuit_2',
    catchup=False,
    schedule=None,
    start_date=pendulum.datetime(2026, 9, 8, tz='UTC'),
    default_args={'retries': 2, 'owner': 'Panda-Singh'},
    tags=['session-4', 'short_circuit'],
)
def pipeline():
    @task()
    def cnt_unanswered_q() -> int:
        return 200_000

    @task.short_circuit
    def check_unanswered_cnt(cnt: int):
        return True if cnt > 0 else False

    @task
    def build_report():
        print("building the product-health report")

    task1 = check_unanswered_cnt(cnt_unanswered_q())
    task1 >> build_report()


pipeline()
