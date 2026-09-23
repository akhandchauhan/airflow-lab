# Session 27 · Reliability & deadlines

**Goal:** move past "just add retries" (Session 04) to the full set of knobs that keep a pipeline honest when things are slow, stuck, or silently late. Retries answer *"the task blipped — try again."* This session answers four different questions retries can't: *"the task is hanging — kill it"* (`execution_timeout`), *"the whole run is wedged — abandon it"* (`dagrun_timeout`), *"stop hammering a struggling service"* (`retry_exponential_backoff` + `max_retry_delay`), *"yesterday failed — don't build today on a hole"* (`depends_on_past`), and the big Airflow 3 addition: *"the run didn't fail, it's just **late** — tell me before the SLA breaks"* (`DeadlineAlert`). Uses BigQuery via `google_cloud_default`.

---

## 1. `retries` fixes blips — these knobs fix everything else

Session 04 gave you `retries` and `retry_delay`: the cure for a transient failure. But a task can hurt you without ever *failing*. It can **hang** — a query that never returns holds a worker slot forever, and no number of retries helps because it never raises. A whole run can **stall** halfway and sit there for hours. A dying service can get **retried into the ground** by a tight retry loop. And a run can **finish fine but two hours late**, blowing an SLA while every task is green. Retries see none of these. The rest of this note is the five knobs that do.

**The analogy — a kitchen on a dinner rush.** `retries` is *"the pan slipped, plate it again."* But you also need: a timer that yanks a dish that's been on the pass too long (`execution_timeout`), a rule that scraps the whole order ticket if it's been open 30 minutes (`dagrun_timeout`), backing off when the fryer is overwhelmed instead of jamming more in (`exponential backoff`), refusing to start tonight's prep if last night's was never finished (`depends_on_past`), and the expo tapping your shoulder when a table has been waiting too long *even though nothing's been dropped* (`DeadlineAlert`).

---

## 2. `execution_timeout` — kill a task that hangs

A per-task `timedelta`. If the task runs longer, Airflow raises `AirflowTaskTimeout`, the task fails (and then obeys its normal `retries`).

```python
from datetime import timedelta

@task(execution_timeout=timedelta(minutes=10))   # ← THE MECHANIC: over 10 min → killed, not hung
def heavy_query() -> int:
    ...
```

- No `execution_timeout` = **no limit**: a wedged query holds its worker slot indefinitely. On a fixed pool (Session 21) one hung task can starve every other DAG.
- Set it to a value comfortably above the task's *normal* p99, not its average — you want to catch "stuck forever," not punish a slow-but-fine day.

---

## 3. `dagrun_timeout` — abandon a wedged run

A per-**DAG** `timedelta` (on `@dag`, not the task). If the whole DAG run is still running after this long, Airflow marks the run failed and stops scheduling new tasks in it.

```python
@dag(dagrun_timeout=timedelta(hours=1), ...)   # ← the entire run gets 1 hour, then it's failed
```

`execution_timeout` bounds **one task**; `dagrun_timeout` bounds **the run end to end** — the safety net for "no single task timed out but the run has been limping through retries for six hours." Set it above the run's normal wall-clock so a healthy run never trips it.

---

## 4. `retry_exponential_backoff` + `max_retry_delay` — back off a struggling service

Fixed `retry_delay` retries at a constant cadence — which, against a service that's *down*, just becomes a steady hammer. Exponential backoff spaces the retries out, giving the service room to recover.

```python
from datetime import timedelta

@task(
    retries=5,
    retry_delay=timedelta(seconds=30),
    retry_exponential_backoff=True,              # ← delay doubles each retry: 30s, 60s, 120s, …
    max_retry_delay=timedelta(minutes=10),       # ← …but never wait longer than 10 min
)
def call_rate_limited_api() -> str:
    ...
```

- `retry_exponential_backoff=True` doubles the wait each attempt, starting from `retry_delay`. (In 3.2+ you can also pass a **float** factor to multiply by something other than 2.)
- **Always pair it with `max_retry_delay`.** Without a ceiling, doubling runs away fast — attempt 15 would schedule *days* out, so your "5 more tries" quietly turns into "retries that never realistically happen." `max_retry_delay` caps the gap.

---

## 5. `depends_on_past` — don't build today on yesterday's hole

A per-task boolean. With `depends_on_past=True`, a task instance will **not start until the same task in the previous data interval succeeded**.

```python
@task(depends_on_past=True)   # ← today waits for yesterday's copy of THIS task to have succeeded
def incremental_load() -> None:
    ...
```

Use it for genuinely sequential state — an incremental load, a running total, anything where day N assumes day N-1 landed. The trap: it can **stall a backfill**, because every run now waits on its predecessor, so one stuck day blocks the entire chain behind it. Reach for it only when order truly matters; leave it off for independent daily snapshots. (Its stricter cousin, `wait_for_downstream`, also waits for the downstream of the previous run — rarely needed.)

---

## 6. Deadline Alerts — the run is *late*, not *failed* (Airflow 3)

Everything above reacts to failure or hang. But the classic SLA miss is a run that is perfectly green and simply **late** — the report that's supposed to be ready by 06:00 finishes at 08:30, and the first you hear of it is a stakeholder, not Airflow. **Deadline Alerts** (AIP-86, the Airflow 3 replacement for the removed SLA feature) fire a callback when a run crosses a time threshold, whether or not it has failed.

Three mandatory parts, all from `airflow.sdk`:

```python
from datetime import timedelta
from airflow.sdk import AsyncCallback, DeadlineAlert, DeadlineReference
from airflow.providers.slack.notifications.slack_webhook import SlackWebhookNotifier

deadline = DeadlineAlert(
    reference=DeadlineReference.DAGRUN_QUEUED_AT,      # ← WHEN to start counting from
    interval=timedelta(minutes=45),                   # ← how long past that reference = "late"
    callback=AsyncCallback(                            # ← WHAT to do when it's late
        SlackWebhookNotifier,
        kwargs={"text": "🚨 {{ dag_run.dag_id }} missed its 45-min deadline"},
    ),
)
```

| Part | Options |
| --- | --- |
| **reference** | `DeadlineReference.DAGRUN_QUEUED_AT`, `DAGRUN_LOGICAL_DATE`, `FIXED_DATETIME(dt)`, `AVERAGE_RUNTIME(max_runs=10, min_runs=10)` |
| **interval** | a `timedelta` before/after the reference |
| **callback** | `AsyncCallback` (runs in the **triggerer**) or `SyncCallback` (runs in the executor), wrapping a Notifier or any importable callable |

Attach it with the DAG's `deadline=` parameter (a single alert, or a **list** for several independent ones). `DeadlineReference.AVERAGE_RUNTIME` is the clever one: instead of a hand-picked interval it derives the deadline from the average of recent successful runs, so "late" means *late for this pipeline* without you tuning a number.

---

## 7. Where each knob lives

| Knob | Goes on | Type | Catches |
| --- | --- | --- | --- |
| `execution_timeout` | task | `timedelta` | one hung task |
| `dagrun_timeout` | `@dag` | `timedelta` | a wedged whole run |
| `retry_exponential_backoff` + `max_retry_delay` | task | `bool`/float + `timedelta` | retrying a down service into the ground |
| `depends_on_past` | task | `bool` | building today on a failed yesterday |
| `deadline` (`DeadlineAlert`) | `@dag` | `DeadlineAlert` \| list | a green-but-late run |

---

## 8. Complete runnable reference DAG (BigQuery via `google_cloud_default`)

One cost-capped query wearing every reliability knob: a task-level `execution_timeout`, exponential backoff with a ceiling, `depends_on_past`, a DAG-level `dagrun_timeout`, and a `DeadlineAlert`.

```python
# dags/stage-6-scale/s27/reliability_demo.py
from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow.sdk import AsyncCallback, DeadlineAlert, DeadlineReference, dag, task
from airflow.providers.slack.notifications.slack_webhook import SlackWebhookNotifier

CONN = "google_cloud_default"
CAP = 200 * 1024 * 1024   # 200 MB maximumBytesBilled ceiling (R17)


@dag(
    dag_id="s27_reliability_demo",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule="@daily",
    catchup=False,
    dagrun_timeout=timedelta(hours=1),                       # whole run bounded end to end
    deadline=DeadlineAlert(                                   # fire if the run is merely LATE
        reference=DeadlineReference.DAGRUN_QUEUED_AT,
        interval=timedelta(minutes=45),
        callback=AsyncCallback(
            SlackWebhookNotifier,
            kwargs={"text": "🚨 {{ dag_run.dag_id }} missed its 45-min deadline"},
        ),
    ),
    tags=["session-27", "reliability"],
    default_args={"owner": "akhand", "retries": 1},
)
def pipeline():

    @task(
        execution_timeout=timedelta(minutes=10),             # kill a hung query
        retries=5,
        retry_delay=timedelta(seconds=30),
        retry_exponential_backoff=True,                      # 30s → 60s → 120s …
        max_retry_delay=timedelta(minutes=10),               # …capped
        depends_on_past=True,                                # today waits on yesterday
    )
    def daily_question_count() -> int:
        from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

        sql = "SELECT COUNT(*) FROM `bigquery-public-data.stackoverflow.posts_questions`"
        hook = BigQueryHook(gcp_conn_id=CONN)
        total = hook.get_first(sql)[0]                       # COUNT → 0 bytes billed anyway
        print(f"questions={total}  (cap={CAP} bytes if this ever scans data)")
        return total

    daily_question_count()


pipeline()
```

```bash
python dags/stage-6-scale/s27/s27_examples.py
airflow dags test s27_reliability_demo 2026-01-01
```

The timeouts and backoff take effect under `airflow dags test`; the **deadline** callback only fires under a running scheduler + triggerer (it needs the triggerer to watch the clock), so treat that line as "wired correctly," verified live rather than in a one-shot test.

---

## 9. Build spec — your challenge (no solution)

**File:** `dags/stage-6-scale/s27/s27_assignment.py` · **dag_id:** `s27_assignment`

Harden a daily Stack Overflow aggregate so it fails **loud and fast**, never hangs, and warns when it's late.

**The problem:**

- One cost-capped BigQuery aggregate task against `bigquery-public-data.stackoverflow` (`COUNT`/aggregate, no `SELECT *`).
- Give the task an `execution_timeout`, `retry_exponential_backoff=True` with a `max_retry_delay` ceiling, and `depends_on_past=True`.
- Give the DAG a `dagrun_timeout` and a `DeadlineAlert` (any `DeadlineReference`) that would fire if the run runs long.

**Constraints:**

- Airflow 3, `airflow.sdk` imports, connection `google_cloud_default`, `maximumBytesBilled` (or a 0-byte aggregate) on the query (R17).
- Passes the integrity gates: `tags`, real `owner`, `retries >= 1`.

**Acceptance criteria:**

- `python dags/stage-6-scale/s27/s27_assignment.py` parses.
- `airflow dags test s27_assignment 2026-01-01` runs green.
- The reliability knobs are present and correctly placed (task-level vs DAG-level per §7).
- `python -m pytest tests/ -v` stays green.

**One nudge (only if stuck):** two of the five knobs go on the **DAG** (`dagrun_timeout`, `deadline`) and three on the **task** — putting `execution_timeout` on the `@dag` or `dagrun_timeout` on the `@task` is the classic mix-up.

---

## 10. Production tip — the retry that turned a wobble into a wait

The bug that pages you at 2am: an API had a bad ten minutes, and the task was set to `retries=20` with `retry_exponential_backoff=True` and **no `max_retry_delay`**. The delays doubled — 1m, 2m, 4m … 17m … and by attempt 15 the next retry was scheduled *four days out*. The service recovered at 02:20, but the task was now asleep until Thursday, the downstream report never ran, and the "self-healing" retries had turned a ten-minute wobble into a multi-day outage nobody could see. The habit that prevents it: **exponential backoff always wears a `max_retry_delay` cap** — pick the longest gap that's still useful (5–15 min for most services) so retries keep actually happening. And bound the run itself with `dagrun_timeout` plus a `DeadlineAlert`, so "this run is going nowhere" pages you at 02:30 instead of surfacing as a missing dashboard at 09:00.

Sources:
[Deadline Alerts — Airflow 3.3](https://airflow.apache.org/docs/apache-airflow/stable/howto/deadline-alerts.html),
[Migrating from SLA to Deadline Alerts — Airflow 3.3](https://airflow.apache.org/docs/apache-airflow/stable/howto/sla-to-deadlines.html),
[Core concepts / Dags — Airflow 3.3](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dags.html),
[Rerunning Dags (depends_on_past) — Astronomer](https://www.astronomer.io/docs/learn/rerunning-dags),
[Configuration Reference — Airflow 3.3](https://airflow.apache.org/docs/apache-airflow/stable/configurations-ref.html)
