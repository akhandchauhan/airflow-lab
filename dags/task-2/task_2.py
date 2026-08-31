from __future__ import annotations

import pendulum
from airflow.sdk import DAG, chain, cross_downstream
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

    # Version C tasks
    extract_c = EmptyOperator(task_id="extract_c")
    vs_c = EmptyOperator(task_id="validate_schema_c")
    vn_c = EmptyOperator(task_id="validate_nulls_c")
    vr_c = EmptyOperator(task_id="validate_ranges_c")
    load_c = EmptyOperator(task_id="load_c")
    notify_c = EmptyOperator(task_id="notify_c")

    validators_c = [vs_c, vn_c, vr_c]
    # extract → all validators (fan-out)
    cross_downstream([extract_c], validators_c)
    # all validators → load (fan-in)
    cross_downstream(validators_c, [load_c])
    chain(load_c, notify_c)                        # load → notify (the tail)
