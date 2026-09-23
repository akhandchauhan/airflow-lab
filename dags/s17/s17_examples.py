"""Session 17 · examples — alerting & callbacks.

Boilerplate is ready. Build the note's reference here (§7): a Product Health pipeline
whose gate can fail, wired to on_failure_callback (alert) and on_success_callback
(all clear); log-based so it runs with no Slack connection, with the SlackNotifier
line shown in a comment. Then run:
    airflow dags test s17_examples 2026-01-01
"""
from __future__ import annotations

import pendulum
from airflow.sdk import dag, task


@dag(
    dag_id="s17_examples",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["session-17"],
    default_args={"owner": "akhand", "retries": 1},
)
def pipeline():
    # TODO: wire on_failure_callback (alert) + on_success_callback (all clear) around a gate task
    @task
    def todo() -> None:
        print("replace me")

    todo()


pipeline()
