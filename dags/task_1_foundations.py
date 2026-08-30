from __future__ import annotations
import pendulum
from airflow.sdk import task, dag


@dag(
    dag_id='01_taskflow_foundations',
    start_date=pendulum.datetime(2026, 8, 30, tz='UTC'),
    catchup=False,
    schedule=None,
    default_args={"owner": "panda singh", "retries": 2},
)
def pipeline():
    @task
    def fetch_orders(multiple_outputs=True):
        return {"gross_revenue": 2500, "order_count": 20, "currency": "USD"}

    @task
    def fetch_refunds() -> float:
        return 34.0

    @task
    def net_revenue(gross_rev: int, refunds: float):
        return float(gross_rev - refunds)

    order_info = fetch_orders()
    refund_cnt = fetch_refunds()
    net_revenue(order_info['gross_revenue'], refund_cnt)


pipeline()
