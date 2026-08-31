from __future__ import annotations

import pendulum
from airflow.sdk import DAG, chain
from airflow.providers.standard.operators.empty import EmptyOperator


with DAG(
    dag_id="02_classic_operators",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["course", "demo"],
    default_args={"owner": "akhand", "retries": 2},
) as dag:

    extract = EmptyOperator(task_id="extract")
    validate_schema = EmptyOperator(task_id="validate_schema")
    validate_nulls = EmptyOperator(task_id="validate_nulls")
    validate_ranges = EmptyOperator(task_id="validate_ranges")
    load = EmptyOperator(task_id="load")
    notify = EmptyOperator(task_id="notify")

    # method 1
    # extract >> [validate_schema, validate_nulls,
    #             validate_ranges] >> load >> notify

    # method 2
    chain(extract, [validate_schema, validate_nulls,
          validate_ranges], load, notify)
