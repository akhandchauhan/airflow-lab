from __future__ import annotations
import pendulum
from airflow.sdk import dag, task, TriggerRule


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

    # skips only the direct children, letting further-down tasks (like an ALL_DONE notify) honor their own trigger rule and still run
    @task.short_circuit(ignore_downstream_trigger_rules=False)
    def check_unanswered_cnt(cnt: int):
        return cnt < 0

    @task
    def build_report():
        print("building the product-health report")

    # run once everyone upstream is finished, no matter how they finished
    @task(trigger_rule=TriggerRule.ALL_DONE)
    def notify():
        print("Notify the user_about the problem")

    task1 = check_unanswered_cnt(cnt_unanswered_q())
    task1 >> build_report() >> notify()


pipeline()
