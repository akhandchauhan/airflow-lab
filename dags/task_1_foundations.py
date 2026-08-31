from __future__ import annotations

import pendulum
from airflow.sdk import task, dag


@dag(
    dag_id='01_taskflow_foundations',
    start_date=pendulum.datetime(2026, 8, 30, tz='UTC'),
    catchup=False,
    schedule=None,
    tags=["course", "w1", "taskflow"],
    default_args={"owner": "panda singh", "retries": 2},
)
def pipeline():
    @task(multiple_outputs=True)
    def fetch_orders():
        return {"gross_revenue": 2500, "order_count": 20, "currency": "USD"}

    @task
    def fetch_refunds() -> float:
        return 34.0

    @task
    def net_revenue(gross_rev: int, refunds: float):
        return float(gross_rev - refunds)

    @task
    def avg_order_value(net_rev: float, order_cnt: int) -> float:
        return round(net_rev / order_cnt, 2)

    @task
    def report(net_rev: float, avg_order_val: float, currency: str, order_cnt: int) -> None:
        # order_cnt is a PARAMETER (resolved value at runtime), not a closure
        # over the XComArg from the enclosing scope.
        print(
            f"net revenue = {net_rev} {currency}, across {order_cnt} orders, aov = {avg_order_val}"
        )

    order_info = fetch_orders()
    refund_cnt = fetch_refunds()
    n_revenue = net_revenue(order_info['gross_revenue'], refund_cnt)
    aov = avg_order_value(n_revenue, order_info['order_count'])
    report(n_revenue, aov, order_info['currency'], order_info['order_count'])


pipeline()
