"""Session 20 · assignment — config-driven, cost-capped tag report.

Boilerplate is ready. Build what the note (§8) asks:
  - a JSON Variable so_report_cfg {tag, lookback_days, max_bytes}
  - read it with Variable.get(deserialize_json=True); capped BigQuery COUNT with tags LIKE
  - reference the same Variable once via Jinja ({{ var.json.so_report_cfg.tag }})
Then run:
    airflow variables set so_report_cfg '{"tag":"python","lookback_days":30,"max_bytes":100000000}'
    airflow dags test s20_assignment 2026-01-01
"""
from __future__ import annotations

import pendulum
from airflow.sdk import dag, task


@dag(
    dag_id="s20_assignment",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["session-20"],
    default_args={"owner": "akhand", "retries": 1},
)
def pipeline():
    # TODO: read JSON Variable (Python + Jinja) -> capped BigQuery COUNT, tags LIKE '%<tag>%'
    @task
    def todo() -> None:
        print("replace me")

    todo()


pipeline()
