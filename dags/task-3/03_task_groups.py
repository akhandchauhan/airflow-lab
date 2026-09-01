from __future__ import annotations

import pendulum
from airflow.sdk import dag, task_group, task


@dag(
    dag_id="03_task_groups",
    start_date=pendulum.datetime(2026, 9, 1, tz='UTC'),
    catchup=False,
    schedule=None,
    args=['task_group', 'override'],
    tags={"owner": "akhand", "retries": 2},
)
