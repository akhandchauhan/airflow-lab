# Session 18 · Observability — the instrument panel

**Goal:** you cannot fix — or trust — what you cannot see, and this session builds the instrument panel that lets you see *inside* a running Airflow. Alerting (Session 17) tells you *that* something broke; observability tells you *what, where, and why*. The one idea to get right: observability is **three separate signals, each answering a different question**, and confusing them is why teams drown in logs yet still can't answer "is the scheduler healthy?" — **logs** (what happened, in words), **metrics** (how much/how fast, as numbers over time), and **lineage** (what data flowed where). You will learn where task and scheduler logs live and how to ship them off the worker, how Airflow emits metrics over StatsD and OpenTelemetry, how OpenLineage records data lineage, and which endpoint tells you a component is alive. This topic is **mostly configuration**, so the reference DAG is deliberately **plain — no BigQuery** (R16): the observability lives in `airflow.cfg` and the cluster, not in DAG code.

---

## 1. Why observability (and the three signals)

Sessions 16–17 gave you a pipeline that stops on bad data and shouts when it fails. But when the shout comes at 3am, the next question is always *"okay — what actually happened?"* Staring at a red task in the grid tells you a task failed; it does not tell you the query timed out, or the scheduler was starved of slots, or an upstream table silently changed schema. Observability is the difference between *"it's broken"* and *"here is the line, the number, and the data path that broke."*

The running analogy: observability is the **cockpit instrument panel** of the pipeline. Three instruments, three jobs:

| Signal | Cockpit instrument | Answers | In Airflow |
| --- | --- | --- | --- |
| **Logs** | the flight recorder | *what happened*, in words | task logs, scheduler/processor logs |
| **Metrics** | the gauges (speed, temp, fuel) | *how much / how fast*, right now and over time | StatsD / OpenTelemetry counters, gauges, timers |
| **Lineage** | the wiring diagram | *what data flowed where* | OpenLineage events |

And a fourth, simpler light — the **health endpoint** — is the "engine OK" indicator: not detail, just *is each component alive*. A pilot who only reads the flight recorder after a crash is flying blind; you want all three instruments live *while* the pipeline runs. The rest of the session is one instrument at a time.

---

## 2. Logs — the flight recorder

**Where task logs live.** By default Airflow uses the `FileTaskHandler`, writing each attempt to a local file under `base_log_folder` (in the `[logging]` section of `airflow.cfg`, defaulting under `AIRFLOW_HOME`). The path is structured, one file per try:

```
dag_id={dag_id}/run_id={run_id}/task_id={task_id}/attempt={try_number}.log
```

(mapped tasks add `map_index={map_index}`). This is why you can open exactly one task attempt in the UI — the layout *is* the addressing scheme, controlled by `log_filename_template`.

**How your task writes to it.** Inside a task, use the standard library `logging` module — Airflow captures the logger's output into that file:

```python
import logging

log = logging.getLogger(__name__)      # ← THE MECHANIC: stdlib logger, Airflow captures it into the task log

@task
def summarize() -> None:
    log.info("starting summary")       # → info line in this attempt's log
    log.warning("row count looks low") # → warning, visible in the UI log view
```

Prefer `log.info(...)` over `print(...)`: you get levels (`DEBUG`/`INFO`/`WARNING`/`ERROR`), timestamps, and the task context for free. The **scheduler**, **DAG processor**, and **triggerer** each write their *own* log streams too — and recall Session 17 §3: **callback errors land in the DAG processor logs, not the task log.**

---

## 3. Remote logging — get logs off the worker

Local log files die with the worker. On Kubernetes or autoscaling, the pod that ran the task is gone by the time you look, taking its logs with it. **Remote logging** ships each finished task's log to durable object storage. Configure it in `[logging]`:

```ini
[logging]
remote_logging = True
remote_base_log_folder = gs://my-airflow-logs/logs   # or s3://…, wasb://…, hdfs://…, oss://…
remote_log_conn_id = google_cloud_default            # the connection with write access
delete_local_logs = True                             # drop the local copy after upload
```

- Supported backends: **S3, GCS, WASB (Azure), HDFS, OSS** — the scheme in `remote_base_log_folder` selects the handler.
- `remote_log_conn_id` names the connection used to write (Session 12) — no credential in config.
- The UI transparently reads from remote storage, so the log view keeps working even after the worker is gone.

Rule of thumb: **any non-trivial deployment turns this on.** Local-only logging is fine on your laptop; in a cluster it is a guaranteed "the logs are gone" incident.

---

## 4. Metrics — the gauges

Logs tell you about *one* run; metrics tell you about *the system over time* — "scheduler heartbeats per second," "tasks currently starving for a pool slot," "p95 task duration this week." Airflow emits these to a metrics backend; you pick one of two protocols in `[metrics]`.

**StatsD** (the classic path — install `apache-airflow[statsd]`):

```ini
[metrics]
statsd_on = True
statsd_host = localhost
statsd_port = 8125
statsd_prefix = airflow
```

**OpenTelemetry** (the modern, vendor-neutral path — install `apache-airflow[otel]`):

```ini
[metrics]
otel_on = True
otel_host = localhost
otel_port = 8889
otel_prefix = airflow
otel_interval_milliseconds = 30000
```

> **What is OpenTelemetry (OTel)?** An open, vendor-neutral standard for emitting metrics/traces/logs, so you are not locked into one monitoring vendor's agent. Airflow can push metrics via OTel to any OTel-compatible collector (Prometheus, Datadog, Grafana Cloud…). Prefer OTel for new setups; StatsD is the older, still-supported route.

Airflow emits three metric shapes, and knowing the shape tells you what to chart:

| Shape | Meaning | Examples |
| --- | --- | --- |
| **Counter** | a running count of events | `ti_failures`, `ti_successes`, `scheduler_heartbeat` |
| **Gauge** | a point-in-time value | `pool.open_slots`, `scheduler.tasks.starving`, `dagbag_size` |
| **Timer** | a duration | `task.duration`, `scheduler.scheduler_loop_duration`, `dagrun.dependency-check` |

The two that catch most incidents early: `scheduler_heartbeat` flatlining (scheduler wedged) and `scheduler.tasks.starving` climbing (not enough slots — Session on pools).

---

## 5. Lineage — the wiring diagram (OpenLineage)

Logs and metrics describe *the tasks*; **OpenLineage** describes *the data* — which inputs a task read and which outputs it wrote, across DAGs and even across tools. When a downstream table is wrong, lineage answers "what fed it?" without you tracing SQL by hand.

> **What is OpenLineage?** An open standard for data lineage. The `apache-airflow-providers-openlineage` provider auto-emits a lineage event per task run (inputs, outputs, run metadata) to a lineage backend like Marquez. You mostly *configure* it, not code it — supported operators report their datasets automatically.

Configure in the `[openlineage]` section:

```ini
[openlineage]
transport = '{"type": "http", "url": "http://marquez:5000"}'   # where events go
namespace = 'product-health-airflow'                           # names THIS Airflow instance
disabled = False                                                # kill switch, no uninstall needed
```

- `transport` (JSON) sets the destination; the shortcut `OPENLINEAGE_URL` env var configures simple HTTP transport.
- `namespace` labels events from this instance (`OPENLINEAGE_NAMESPACE` is the env equivalent).
- `disabled = True` (or `OPENLINEAGE_DISABLED=true`) turns emission off without removing the provider.

For BigQuery tasks (the spine) OpenLineage captures the source and destination tables automatically — so the Product Health pipeline's lineage graph builds itself once the provider is on.

---

## 6. Health endpoints — the "engine OK" light

The simplest instrument: *is each component alive?* Airflow exposes an HTTP health endpoint on the API server:

```
GET /api/v2/monitor/health
```

```json
{
  "metadatabase": {"status": "healthy"},
  "scheduler":    {"status": "healthy", "latest_scheduler_heartbeat": "..."},
  "triggerer":    {"status": "healthy", "latest_triggerer_heartbeat": "..."},
  "dag_processor":{"status": "healthy", "latest_dag_processor_heartbeat": "..."}
}
```

- Each component is `healthy` or `unhealthy`; scheduler/triggerer/dag_processor go unhealthy when their heartbeat is stale (>30s by default). Point your load balancer or uptime monitor at this.
- An **independent scheduler health server** (no DB round-trip) runs on port **8974** when `[scheduler] enable_health_check = True` — returns HTTP 200 healthy / 503 unhealthy, ideal for a Kubernetes liveness probe.
- **CLI checks** for scripts and probes: `airflow jobs check --job-type SchedulerJob --local` (scheduler alive) and `airflow db check` (metadata DB reachable).

The distinction from §4: metrics tell you *how well* things run; the health endpoint tells you *whether they are running at all* — and it is the first thing a monitor should poll.

---

## 7. Reference DAG — plain, logging in action

> **Plain-DAG session (R16):** observability is cluster/config, not DAG code, so this reference uses **no BigQuery and no connection**. It exists to show the one observability lever you *do* pull from inside a DAG — structured logging at the right level — so it lands in the task log (and, with §3 on, in remote storage).

```python
# dags/stage-5-quality/s18/s18_examples.py
from __future__ import annotations

import logging

import pendulum
from airflow.sdk import dag, task

log = logging.getLogger(__name__)


@dag(
    dag_id="s18_examples",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["session-18", "observability"],
    default_args={"owner": "akhand", "retries": 1},
)
def pipeline():

    @task
    def emit_signals() -> None:
        # each level lands in this attempt's task log; WARNING+ stands out in the UI
        log.debug("debug: verbose detail, usually filtered out")
        log.info("info: pipeline step started")
        log.warning("warning: a metric looks low — worth a glance")
        log.error("error: this is how a real failure reads in the log")
        # in prod, remote_logging ships THIS file to gs://…/logs after the task ends (§3)
        print("print() also lands in the log, but log.info() carries level + timestamp")

    emit_signals()


pipeline()
```

```bash
python dags/stage-5-quality/s18/s18_examples.py
airflow dags test s18_examples 2026-01-01
```

Open the task log in the UI: you will see the `INFO`/`WARNING`/`ERROR` lines with levels and timestamps. Then hit `http://<your-airflow>/api/v2/monitor/health` to see all four components report healthy — logs from the DAG, health from the endpoint, side by side.

---

## 8. Build spec — your challenge (no solution)

**File:** `dags/stage-5-quality/s18/s18_assignment.py` · **dag_id:** `s18_assignment`

Instrument a small plain DAG and prove you can read it from the outside.

**The problem:**

- A `@task` uses the stdlib `logging` logger to emit at **INFO**, **WARNING**, and **ERROR** — each line meaningful (a step start, a soft anomaly, a simulated failure detail).
- A second `@task` logs a short "summary" line a human could scan in the grid.
- In a top-of-file docstring or comments, write the **exact `[logging]` config** you would add to ship these logs to GCS/S3 remotely (§3), and the **health endpoint URL** you would point a monitor at (§6).

**Constraints:**

- Plain DAG — **no BigQuery, no connection** (this is a config-focused session, R16).
- Prefer `log.info()` over `print()`; use the right level for each message.
- Names clearly distinct (R12): `task_id` a noun, function a verb form, variable its role.
- Passes the integrity gates: `tags`, real `owner`, `retries >= 1`.

**Acceptance criteria:**

- `python dags/stage-5-quality/s18/s18_assignment.py` parses (prints nothing).
- `airflow dags test s18_assignment 2026-01-01` runs green.
- The task log shows the INFO/WARNING/ERROR lines with levels.
- Your docstring names the `[logging]` remote-logging keys and the `/api/v2/monitor/health` endpoint.
- `python -m pytest tests/ -v` stays green.

**One nudge (only if stuck):** you do not configure remote logging or metrics *in the DAG* — those are `airflow.cfg`/env settings. The DAG's only job here is to emit well-levelled logs; the rest is what you *document*.

---

## 9. Production tip — blind in the cloud when the logs vanished

The bug that pages you at 2am: a task failed on a Kubernetes worker, you clicked its log in the UI, and got **"log file does not exist."** The pod that ran it was reaped seconds after it failed, and with local-only logging the log died with the pod. You now have a red task, an angry stakeholder, and *nothing to read* — the flight recorder went down with the plane.

- **Turn on remote logging before you need it.** `remote_logging = True` to durable storage (§3) is the difference between reading the traceback and guessing. On any autoscaling or K8s deployment this is not optional.
- **Watch the leading-indicator metrics, not just failures.** `scheduler_heartbeat` flatlining or `scheduler.tasks.starving` creeping up warns you *before* tasks pile red. Alerting on failures is lagging; metrics are leading.
- **Point a monitor at `/api/v2/monitor/health`, not at the login page.** A green login page means the web server is up; it says nothing about the scheduler. The health endpoint is the only thing that reports each component — poll *that*.
- **A log you can't find is a log you don't have.** Structured levels + remote storage + a queryable backend turn 3am archaeology into a two-minute search. The time to build the instrument panel is *before* the incident, never during.

---

Sources:
[Logging for tasks — Airflow 3.3](https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/logging-monitoring/logging-tasks.html),
[Metrics (StatsD & OpenTelemetry) — Airflow 3.3](https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/logging-monitoring/metrics.html),
[Checking Airflow health status — Airflow 3.3](https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/logging-monitoring/check-health.html),
[OpenLineage user guide — Airflow OpenLineage provider](https://airflow.apache.org/docs/apache-airflow-providers-openlineage/stable/guides/user.html),
[OpenLineage configuration reference — Airflow OpenLineage provider](https://airflow.apache.org/docs/apache-airflow-providers-openlineage/stable/configurations-ref.html)
