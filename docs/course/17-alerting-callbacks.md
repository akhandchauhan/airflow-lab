# Session 17 · Alerting & Callbacks — make the pipeline shout

**Goal:** a pipeline that fails at 3am and tells no one is worse than no pipeline, because you *think* it ran. This session wires the mouth onto the machine: **callbacks** — the hooks Airflow fires on success, failure, retry, or a missed deadline — and **notifiers** — the reusable objects that turn "a callback fired" into a Slack message, a page, an email. The one idea to get right: a callback is a **socket on the task, not a task itself**; it runs *outside* the task's own success/failure, in a place with different rules about where its logs land and what happens when it throws. Get that, and you will never again ship an alert that silently fails to alert. You will learn every callback hook, exactly where each one executes, how to write a `BaseNotifier`, how to fire a `SlackNotifier`, and why Airflow 3 replaced `sla` with **Deadline Alerts**. Reference DAG on the Stack Overflow spine that alerts when a quality gate trips.

---

## 1. Why callbacks exist (the pipeline that failed silently)

Session 16 gave you a circuit breaker that trips on bad data. But a tripped breaker in an empty house helps no one — someone has to *hear* it. Retries (Session 05) buy you automatic recovery; when the retries are exhausted and the task is finally, truly failed, the pipeline needs to **reach a human**. Without that, the standard failure mode is: the run goes red in a UI nobody is watching, the on-call finds out at 9am when the dashboard is stale, and the post-mortem question is always the same — *"why didn't we know sooner?"*

The running analogy for the session: a callback is a **socket bolted onto the machine**, and a notifier is the **alarm you plug into it**. The machine (your task) has several sockets — one labelled "on failure," one "on success," one "on retry." You clip an alarm into the socket you care about; when the machine hits that state, current flows to the socket and the alarm sounds. The machine does not know or care *what* alarm is plugged in — a Slack horn, a PagerDuty siren, a plain log buzzer. That separation is the whole design: **the task carries the socket, the notifier is the swappable alarm.**

The rule that prevents the classic mistake: **a callback is not part of the task.** It fires *after* the task reaches its final state, in a separate step. That means a callback cannot fail the task (the task already succeeded or failed), and — the part that bites — a bug *inside* your callback does not turn the run red. It fails quietly, somewhere you are not looking. §3 is where that lands.

---

## 2. The callback hooks — every socket on the machine

Airflow 3 gives a task (and, for some, a DAG) five callback hooks. Each is a parameter you set to a callable — or a **list** of callables — on the operator, the `@task`, or `default_args`:

| Callback | Fires when… | Typical use |
| --- | --- | --- |
| `on_execute_callback` | right **before** the task starts executing | mark a start, warm a cache |
| `on_success_callback` | the task (or DAG) **succeeds** | "all clear" notification, downstream signal |
| `on_failure_callback` | the task (or DAG) **fails** (after retries exhausted) | **the alert** — page/Slack |
| `on_retry_callback` | the task is **up for retry** | noisy-neighbour warning, backoff logging |
| `on_skipped_callback` | the task raises `AirflowSkipException` | distinguish "skipped on purpose" from failed |

Two facts that matter:

- **Callbacks accept a list.** `on_failure_callback=[notify_slack, notify_pager]` fires both — you do not have to choose one alarm.
- **Every callback receives the `context`** — the same run-context mapping you get from `get_current_context()` (Session 09), carrying `task_instance`, `dag_run`, `logical_date`, and the exception. A minimal callback:

```python
def alert_on_failure(context) -> None:          # ← THE MECHANIC: one arg, the run context
    ti = context["task_instance"]
    print(f"ALERT: {ti.dag_id}.{ti.task_id} failed at {context['logical_date']}")
```

Wire it where you want the socket:

```python
from airflow.sdk import dag, task

@dag(..., default_args={"on_failure_callback": alert_on_failure})   # every task inherits the socket
def pipeline():

    @task(on_failure_callback=alert_on_failure)                    # or on one specific task
    def load() -> None:
        ...
```

Set it in `default_args` for a DAG-wide alarm, or on a single task for a targeted one. `on_failure_callback` on the `@dag` itself fires when the **DAG run** fails.

---

## 3. Where callbacks run — the gotcha that eats alerts

This is the single most important slide in the session, because getting it wrong means your alert *looks* wired but never reaches anyone.

- **A callback runs outside the task's own log stream.** Per the Airflow docs: *"Errors in callback functions will show up in dag processor logs rather than task logs."* So if your Slack call throws — bad token, wrong channel, network blip — the traceback lands in the **DAG processor logs**, not the red task's log where you'd naturally look. You will swear the callback "didn't fire"; it fired and crashed somewhere you never opened.
- **A callback exception does not fail the run.** The task already has its final state. A broken alarm therefore fails *silently* — the worst possible failure for an alerting system.
- **Deadline Alert callbacks run in yet another place** (§6): an `AsyncCallback` runs in the **Triggerer**, a `SyncCallback` runs in the **executor**. Same lesson, more locations to check.

The practical consequence: **test the alarm by forcing a real failure**, and confirm the message actually arrives — never assume a wired callback works because the code parses. An untested `on_failure_callback` is a smoke detector you never pressed the button on.

---

## 4. Notifiers — the reusable alarm (`BaseNotifier`)

A raw callback function is fine for one-off logging, but "send a Slack message with the failed task, dag, and time" is something you want *once*, reused everywhere, with templated fields. That reusable object is a **Notifier**, and you build one by subclassing `BaseNotifier`:

```python
from airflow.sdk import BaseNotifier      # ← Airflow 3 public API

class BacklogNotifier(BaseNotifier):
    template_fields = ("message",)                        # ← these get Jinja-rendered per run

    def __init__(self, message: str):
        self.message = message

    def notify(self, context) -> None:                    # ← THE MECHANIC: the one method you implement
        ti = context["task_instance"]
        print(f"[{ti.dag_id}.{ti.task_id}] {self.message}")
```

- **`notify(self, context)`** is the only required method — it does the sending. You get the same `context` as any callback.
- **`template_fields`** lists attributes rendered as Jinja templates, so `BacklogNotifier(message="failed on {{ ds }}")` gets the real date at run time — the notifier is templated exactly like an operator.
- A notifier **is** a callback: you pass an *instance* where a callback is expected — `on_failure_callback=BacklogNotifier("the load failed")`. That is why the whole provider ecosystem ships notifiers instead of loose functions.

---

## 5. The Slack alarm — `SlackNotifier`

The Slack provider ships a ready-made notifier so you never write the HTTP call yourself:

```python
from airflow.providers.slack.notifications.slack import SlackNotifier   # send_slack_notification is the same object
```

> **What is `apache-airflow-providers-slack`?** The provider that bundles Slack hooks, operators, and notifiers. `SlackNotifier` (aliased `send_slack_notification`) is a `BaseNotifier` subclass — it reads its token from a **Slack connection** (`slack_conn_id`, default `slack_api_default`) so no secret sits in your DAG (Session 15's rule).

Wire it straight into a callback list; every field is Jinja-templated:

```python
from airflow.providers.slack.notifications.slack import SlackNotifier

@task(
    on_failure_callback=[
        SlackNotifier(
            slack_conn_id="slack_api_default",
            text="🚨 {{ ti.dag_id }}.{{ ti.task_id }} failed on {{ ds }}",
            channel="#data-alerts",
            username="Airflow",
        )
    ],
)
def publish_health() -> None:
    ...
```

For an incoming-webhook setup rather than the Web API, the sibling is `SlackWebhookNotifier` (`airflow.providers.slack.notifications.slack_webhook`). Pick whichever matches how your Slack app is configured — behaviour is the same, only the connection type differs.

---

## 6. Deadlines — `sla` is gone, meet Deadline Alerts

In Airflow 2 you set `sla=timedelta(...)` and got an `sla_miss_callback`. **Airflow 3 removed SLAs** and replaced them with **Deadline Alerts** (shipped in 3.1, UI polished in 3.2). The reason: the old SLA was tied to the *logical date* and fired confusingly on backfills; Deadline Alerts let you anchor the clock to a real reference point and pick where the callback runs.

A Deadline Alert is three things: a **reference** (when the clock starts), an **interval** (how long is too long), and a **callback** (what to do). All from `airflow.sdk`:

```python
from datetime import timedelta
from airflow.sdk import DAG, DeadlineAlert, DeadlineReference, AsyncCallback
from airflow.providers.slack.notifications.slack_webhook import SlackWebhookNotifier

with DAG(
    dag_id="s17_deadline_example",
    deadline=DeadlineAlert(                               # ← THE MECHANIC: reference + interval + callback
        reference=DeadlineReference.DAGRUN_QUEUED_AT,     # start the clock when the run is queued
        interval=timedelta(minutes=15),                  # 15 min later, if not done → fire
        callback=AsyncCallback(SlackWebhookNotifier, kwargs={"text": "run overran 15m"}),
    ),
):
    ...
```

- **`DeadlineReference`** options: `DAGRUN_QUEUED_AT`, `DAGRUN_LOGICAL_DATE`, `FIXED_DATETIME(dt)`, and `AVERAGE_RUNTIME(max_runs=..., min_runs=...)` (anchor to the historical average — "alert if today runs slower than usual").
- **`AsyncCallback`** runs in the **Triggerer** (non-blocking, preferred); **`SyncCallback`** runs in the **executor**. (Note: the import moved to `airflow.sdk` in Airflow 3.2 from `airflow.sdk.definitions.deadline`.)
- Pass a **list** — `deadline=[DeadlineAlert(...), DeadlineAlert(...)]` — for tiered "warn at 15m, page at 60m" alerting.

---

## 7. Complete runnable reference — alert when the gate trips

One DAG on the Product Health spine: a quality gate (Session 16) that will fail on a bad threshold, wired to an `on_failure_callback` that alerts, plus an `on_success_callback` "all clear." The callback logs so the DAG is runnable with **no Slack connection required** — the comment shows exactly where `SlackNotifier` drops in. Read-only, cost-capped, no `SELECT *`.

```python
# dags/s17/s17_examples.py
from __future__ import annotations

import pendulum
from airflow.sdk import dag, task
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

CONN = "google_cloud_default"
QUESTIONS = "bigquery-public-data.stackoverflow.posts_questions"


def alert_on_failure(context) -> None:
    # In prod, swap this for a SlackNotifier instance on on_failure_callback:
    #   on_failure_callback=[SlackNotifier(slack_conn_id="slack_api_default",
    #       text="🚨 {{ ti.dag_id }}.{{ ti.task_id }} failed", channel="#data-alerts")]
    ti = context["task_instance"]
    exc = context.get("exception")
    print(f"🚨 ALERT: {ti.dag_id}.{ti.task_id} failed on {context['logical_date']} — {exc}")


def clear_on_success(context) -> None:
    ti = context["task_instance"]
    print(f"✅ {ti.dag_id}.{ti.task_id} succeeded — Product Health published")


@dag(
    dag_id="s17_examples",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["session-17", "alerting", "stackoverflow"],
    default_args={"owner": "akhand", "retries": 1, "on_failure_callback": alert_on_failure},
)
def pipeline():

    @task
    def measure_answer_rate() -> float:
        hook = BigQueryHook(gcp_conn_id=CONN, use_legacy_sql=False, location="US")
        answered, total = hook.get_first(
            f"SELECT COUNTIF(answer_count>0), COUNT(*) FROM `{QUESTIONS}` "
            f"WHERE creation_date >= '2022-01-01'"
        )
        rate = round(100 * answered / total, 2)
        print(f"answer_rate = {rate}%")
        return rate

    @task
    def gate_and_publish(rate: float) -> None:
        # deliberately strict so you can watch the failure alarm fire when you tighten it
        if rate < 50.0:
            raise ValueError(f"answer_rate {rate}% below floor — refusing to publish")
        print(f"publishing Product Health: answer_rate={rate}%")

    publish = gate_and_publish.override(on_success_callback=clear_on_success)
    publish(measure_answer_rate())


pipeline()
```

```bash
python dags/s17/s17_examples.py
airflow dags test s17_examples 2026-01-01
```

On real data the answer rate clears the floor, the run is green, and `clear_on_success` logs the "all clear." Raise the floor in `gate_and_publish` to `99.0`, rerun, and watch `alert_on_failure` fire — then look for its output in the **DAG processor logs**, not the task log (§3).

---

## 8. Build spec — your challenge (no solution)

**File:** `dags/s17/s17_assignment.py` · **dag_id:** `s17_assignment`

Build a Product Health pipeline that **alerts on failure** and clears on success.

**The problem:**

- A `@task` computes a metric on the Stack Overflow tables (answer rate, unanswered backlog, top-tag drift — your pick) via `BigQueryHook`.
- A `@task` gates that metric and **raises** when it is out of band (so failure is reachable).
- Wire an **`on_failure_callback`** — either a plain function *or* a `BaseNotifier` subclass you write — that alerts with the dag_id, task_id, and the exception from `context`.
- Wire an **`on_success_callback`** that logs an "all clear."
- In a comment, show the exact `SlackNotifier(...)` line that would replace your log callback in production (with `slack_conn_id`, `text` templated, `channel`).

**Constraints:**

- No secret in the file — a real Slack alert reads `slack_conn_id`, never a hardcoded token (Session 15).
- Every query capped with `maximumBytesBilled`; no `SELECT *`; `"useLegacySql": False`.
- Names clearly distinct (R12): `task_id` a noun, callback a verb form, variable its role.
- Passes the integrity gates: `tags`, real `owner`, `retries >= 1`.

**Acceptance criteria:**

- `python dags/s17/s17_assignment.py` parses (prints nothing).
- `airflow dags test s17_assignment 2026-01-01` runs green on real data; the success callback logs.
- Forcing the gate to fail makes the **failure callback fire** — and you can find its output in the DAG processor logs.
- `python -m pytest tests/ -v` stays green.

**One nudge (only if stuck):** a `BaseNotifier` subclass *is* a valid `on_failure_callback` value — pass an instance, don't call it yourself; Airflow invokes `notify(context)` for you.

---

## 9. Production tip — the alert that never alerted

The bug that pages you at 2am is the one that *should* have paged you at 2am and didn't. A team wired `on_failure_callback=SlackNotifier(...)` and slept soundly for months. Then a real failure hit — and no Slack message came. The pipeline had been failing *silently* on the alert itself: the Slack token had rotated, the notifier threw an auth error, and — because **callback exceptions land in the DAG processor logs, not the task log, and never fail the run (§3)** — nobody saw the broken alarm. The circuit breaker tripped into a horn that had no batteries.

- **Test the alarm, not just the wiring.** Force a real failure on purpose and confirm the message actually arrives. A callback that parses is not a callback that alerts.
- **Watch the right logs.** When an alert "didn't fire," open the **DAG processor logs** first — that is where a crashing callback hides.
- **Keep the alarm dumb and dependency-light.** The more your notifier does (formatting, lookups, extra API calls), the more ways it fails while everything else is already on fire. A failing alert path is the worst time for clever code.
- **Alert on the DAG, page on the metric.** Use `on_failure_callback` for "the pipeline broke" and a Deadline Alert (§6) for "the pipeline is *late*" — a run that hangs never fails, so a failure callback alone will never tell you it's stuck.

---

Sources:
[Callbacks — Airflow 3.3](https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/logging-monitoring/callbacks.html),
[Creating a notifier (BaseNotifier) — Airflow 3.3](https://airflow.apache.org/docs/apache-airflow/stable/howto/notifications.html),
[Slack notifier how-to — Airflow Slack provider](https://airflow.apache.org/docs/apache-airflow-providers-slack/stable/notifications/slack_notifier_howto_guide.html),
[Deadline Alerts — Airflow 3.3](https://airflow.apache.org/docs/apache-airflow/stable/howto/deadline-alerts.html),
[Migrating from SLA to Deadline Alerts — Airflow 3.3](https://airflow.apache.org/docs/apache-airflow/stable/howto/sla-to-deadlines.html)
