# Session 11 · Event-driven scheduling (the ring comes from outside Airflow)

**Goal:** Every asset event so far came from *inside* Airflow — a task in your instance succeeded and rang the bell (§12), or several bells combined into a condition (§13). But the real world doesn't wait for your tasks: a file lands in S3, an order hits an SQS queue, a Kafka topic gets a message — and none of that is an Airflow task succeeding. This session closes the loop: an **`AssetWatcher`** attaches to an asset and listens to an **external message queue**, turning each incoming message into an asset event your DAGs can schedule on. The trigger originates *outside Airflow entirely*. We also add the safety floor — **`AssetOrTimeSchedule`** — so a silent queue still gives you a scheduled run. Asset names stay on the Stack Overflow spine. _(Stage 3 — react to data, not the clock — and now, not even to your own tasks.)_

---

## 1. The gap §12 left open

Session 09's rule was blunt and worth repeating: **Airflow does not watch your data — it watches whether a task succeeded.** An asset event is written when a task with that asset in `outlets` finishes. That's the doorbell: *your* task rings it.

But most upstream systems aren't Airflow tasks. A vendor drops a file in a bucket. A payments service pushes to SQS. A Kafka topic streams change events. Airflow, on its own, **cannot see any of that** — the docs are explicit that asset updates otherwise happen only "by tasks in the same Airflow instance completing successfully, manually through the Airflow UI, or through a call to the Airflow REST API." A queue is none of those. Before event-driven scheduling your only options were the bad old ones from §12: a sensor burning a worker slot polling the queue, or an external cron poking the REST API. Both are the "consumer must know when/who" trap assets were built to kill.

**The doorbell, extended.** §12 was your own courier ringing the bell when *they* finished. Event-driven scheduling is wiring the doorbell to the **building's front-gate intercom** — when *anyone outside* buzzes the gate (a message hits the queue), your bell rings, even though no task of yours ran. Same bell, same listeners downstream; a new *source* for the ring.

---

## 2. How it differs from §12 — the one table to internalize

This is the crux of the session. Both produce an asset event; the **origin** is the whole difference, and it changes how you reason about triggers.

| Dimension            | §12 — task produces the asset                     | §14 — watcher on an external queue                                     |
| -------------------- | ------------------------------------------------- | ---------------------------------------------------------------------- |
| **What rings it**    | an Airflow **task succeeds** (`outlets=[asset]`)  | a **message arrives** on an external queue (SQS, Kafka, …)             |
| **Where it lives**   | inside your Airflow instance                       | outside Airflow — a vendor, a service, another system                  |
| **Who declares it**  | the producer **DAG** (`@task(outlets=…)`)         | the **asset itself** (`Asset(..., watchers=[AssetWatcher(...)])`)      |
| **The payload**      | producer sets `extra={...}` deliberately          | the **message body** becomes the event's `extra` automatically         |
| **Runs on**          | the producing task's schedule                     | a long-running **triggerer** process polling the source                |
| **Fails silently?**  | if the producer DAG is paused/broken              | if the queue is empty, the connection is down, or the triggerer is off |

The mental shift: in §12 you *own* the ring — you decide in code when to write the event. In §14 you **subscribe** to a ring you don't control; your job is only to describe *what to listen to* and *what to do when it fires*.

---

## 3. The moving parts — trigger, watcher, asset, triggerer

Event-driven scheduling is four pieces stacked. Know what each is:

1. **A trigger** — an async object that watches an external source and yields a `TriggerEvent` when something happens. For queues, Airflow ships **`MessageQueueTrigger`** in the `common.messaging` provider. It must inherit from **`BaseEventTrigger`** (not the generic `BaseTrigger`) — only those are allowed as watchers, to avoid infinite rescheduling loops. `MessageQueueTrigger` supports **Amazon SQS and Apache Kafka** out of the box today, with more queues planned.
2. **An `AssetWatcher`** — the glue: it gives the trigger a name and binds it to an asset. `AssetWatcher(name="...", trigger=<trigger>)`.
3. **The `Asset`** — declares its watchers: `Asset("name", watchers=[AssetWatcher(...)])`. **This is the new part** — in §12 the asset was passive; here the asset actively listens.
4. **The triggerer** — the long-running Airflow process that actually runs async triggers. Event-driven assets **require a running triggerer**; no triggerer, no listening, no events. (You already have one if you've used deferrable operators.)

`MessageQueueTrigger`, `TriggerEvent`, and `BaseEventTrigger` are **not stdlib and not `airflow.sdk`** — `MessageQueueTrigger` comes from the pinned provider `apache-airflow-providers-common-messaging`; `AssetWatcher`, `Asset`, `dag`, `task` come from `airflow.sdk`. Explaining the provider once (R6): `common.messaging` is Airflow's queue-abstraction provider — one trigger class that speaks several queue dialects via a `scheme`, so your DAG code doesn't hard-bind to the SQS or Kafka SDK.

---

## 4. `TriggerEvent` — how the message becomes an asset event

When the trigger sees a new message, it emits a **`TriggerEvent`**, and Airflow does two things: it writes an **`AssetEvent`** for the watched asset (so any DAG scheduled on it runs), and it **attaches the trigger's payload to that event's `extra` dictionary**. So the queue message body arrives at your consumer exactly where §12's producer-set metadata did — `triggering_asset_events[asset][-1].extra` — you just didn't write it; the message did.

```python
@task
def handle(**context) -> None:
    for asset, events in context["triggering_asset_events"].items():   # ← THE MECHANIC
        payload = events[-1].extra          # the external message body, verbatim
        print(f"queue message for {asset.name}: {payload}")
```

That symmetry is the payoff: downstream code reads a watcher-driven event with the **same API** as a task-driven one (§12.7a). A consumer usually doesn't need to know or care whether the ring came from your task or the front gate — it just reads `extra`.

---

## 5. `AssetOrTimeSchedule` — the floor for a queue that goes quiet

An external queue can go silent for reasons you don't control: the vendor's cron slipped, the topic drained, a network partition. A pure event-driven consumer then simply never runs — and, as in §13's war story, *silence is the worst failure mode*. `AssetOrTimeSchedule` gives you a **time floor**: run when the watched asset fires, **or** on a clock tick as a guaranteed heartbeat.

```python
from airflow.timetables.assets import AssetOrTimeSchedule
from airflow.timetables.trigger import CronTriggerTimetable

@dag(
    schedule=AssetOrTimeSchedule(                                    # ← THE MECHANIC
        timetable=CronTriggerTimetable("0 * * * *", timezone="UTC"), # hourly floor
        assets=sqs_orders_asset,                                     # …or the instant a message lands
    ),
    ...
)
def process_orders():
    ...
```

Inside the task, `triggering_asset_events` tells you *why* this run happened: **non-empty ⇒ a real message drove it; empty ⇒ it was the hourly floor** (nothing arrived, run anyway to check / stay warm). Same tool introduced in §13.6 — here it guards an *external* source rather than a sibling DAG, which is exactly when you need it most, because you control the queue even less than you control your own producers. `assets=` takes a single asset or the full `&`/`|` expression tree from §13.

---

## 6. Complete runnable reference (shape you deploy; needs a triggerer + connection to fire)

An SQS-watched asset and a consumer that reads the message payload. Unlike §12–13 this **cannot fully "run" from `dags test`** — a real message on a real queue is what fires it — so the reference is the **deployable shape**; type it into `dags/stage-3-assets/s11/s11_examples.py`. It parses clean and passes integrity gates as-is.

```python
from __future__ import annotations

import pendulum
from airflow.providers.common.messaging.triggers.msg_queue import MessageQueueTrigger
from airflow.sdk import Asset, AssetWatcher, dag, task

# a trigger that listens to an EXTERNAL SQS queue — nothing here is an Airflow task
sqs_trigger = MessageQueueTrigger(
    scheme="sqs",
    sqs_queue="https://sqs.us-east-1.amazonaws.com/0123456789/stackoverflow-ingest",
)

# the asset actively LISTENS via a watcher — the new §14 idea
orders_queue = Asset(
    uri="x-so-ingest-queue",
    name="so_ingest_queue",
    watchers=[AssetWatcher(name="sqs_watcher", trigger=sqs_trigger)],   # ← THE MECHANIC
)


@dag(
    dag_id="s11_examples",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=[orders_queue],                     # runs when a MESSAGE arrives, not when a task ran
    catchup=False,
    tags=["session-11"],
    default_args={"owner": "akhand", "retries": 1},
)
def pipeline():
    @task
    def handle_message(**context) -> None:
        fired = context["triggering_asset_events"]
        for asset, events in fired.items():
            print(f"external message on {asset.name}: {events[-1].extra}")   # message body

    handle_message()


pipeline()
```

```bash
python dags/stage-3-assets/s11/s11_examples.py     # parses; asset shows a watcher in the UI Assets view
# to actually fire it: run a triggerer, define the SQS connection, and drop a message on the queue
```

To see it fire you need three things live: a **running triggerer**, the queue **connection** defined (e.g. `aws_default` for SQS), and an actual **message** on the queue. Then the watcher turns that message into an asset event and `s11_examples` runs — with no producer DAG anywhere.

> **API note — exact `MessageQueueTrigger` kwargs vary by provider version.** The official `common.messaging` docs show `MessageQueueTrigger(scheme="sqs", sqs_queue="...")` (used above); some Astronomer examples use `MessageQueueTrigger(queue="...", aws_conn_id="aws_default", waiter_delay=30)`. Pin the provider and check *its* `triggers.html` for the constructor your version accepts. The **`AssetWatcher` / `Asset(watchers=...)` / `schedule=[asset]`** shape is stable; only the trigger's own kwargs move.

---

## 7. Build spec — your challenge (no solution)

**File:** `dags/stage-3-assets/s11/s11_assignment.py` · **dag_id:** `s11_assignment`

Build a consumer that reacts to an **external queue message**, with a **daily time floor** so a silent queue still runs.

**The problem:**

- Define a `MessageQueueTrigger` for an external queue (SQS or Kafka — your choice of `scheme`).
- Define an `Asset` on the spine (e.g. `so_votes_queue`) whose `watchers=[AssetWatcher(name=..., trigger=...)]` listens to that trigger.
- Schedule `s11_assignment` on an **`AssetOrTimeSchedule`**: `assets=` your watched asset, `timetable=` a `CronTriggerTimetable` daily tick.
- One task that logs the message payload from `triggering_asset_events` **and** prints whether this run was message-driven (payload present) or the time floor (no triggering events).

**Constraints:**

- `MessageQueueTrigger` from `airflow.providers.common.messaging.triggers.msg_queue`; `AssetWatcher` / `Asset` / `dag` / `task` from `airflow.sdk`; `AssetOrTimeSchedule` from `airflow.timetables.assets`.
- Do **not** write a producer task that outlets the asset — the whole point is the trigger comes from **outside** Airflow (contrast §12).
- Passes integrity gates: non-empty `tags`, real `owner`, `retries >= 1`, and **parses clean** (`python dags/stage-3-assets/s11/s11_assignment.py`) with no live queue.

**Acceptance criteria:**

- `python dags/stage-3-assets/s11/s11_assignment.py` parses with no errors.
- The UI **Assets** view shows the asset **with a watcher** and `s11_assignment` as its consumer.
- `python -m pytest tests/ -v` stays green.
- (If you have a triggerer + connection + real queue:) a message fires the DAG with the body in `extra`; with no message, the daily floor still produces one run whose `triggering_asset_events` is empty.

**One nudge (only if stuck):** the schedule is `schedule=AssetOrTimeSchedule(timetable=CronTriggerTimetable("0 5 * * *", timezone="UTC"), assets=votes_queue)`. Detect the floor by checking `if not context["triggering_asset_events"]:`.

---

## 8. Production tip — the queue that "worked in the demo" and slept in prod (2am)

The page: an event-driven ingest DAG hadn't run in 14 hours and the freshness SLA blew. No task failed — because no task ever *started*. Three classic causes, all silent: the **triggerer wasn't running** in prod (it worked locally where you'd started one by hand), so nothing was listening; the queue **connection** was missing, so the watcher couldn't poll; and there was **no time floor**, so "nothing listening" looked identical to "no messages." Event-driven scheduling moves your failure surface *outside* Airflow — the exact place your DAG-level alerting doesn't look.

- **Always pair a watcher with `AssetOrTimeSchedule`.** A daily/hourly floor converts "silently never ran" into "ran on the floor with empty `triggering_asset_events`" — a *visible, alertable* signal. Never deploy a bare `schedule=[watched_asset]` for anything with an SLA.
- **Treat the triggerer and the connection as part of the DAG.** A watcher DAG has three hidden prerequisites — triggerer up, connection defined, queue reachable — none of which show as a task failure. Add a liveness check on the triggerer and alert on "no run in N hours," because the failure is *absence*, not error.
- **The payload is untrusted external input.** In §12 you wrote `extra` yourself; here it's a message from another system. Validate it in the task (shape, required keys) before acting — a malformed message shouldn't silently become a malformed run.

---

Sources:
[Event-driven scheduling — Airflow 3.3](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/event-scheduling.html),
[Messaging Triggers — apache-airflow-providers-common-messaging](https://airflow.apache.org/docs/apache-airflow-providers-common-messaging/stable/triggers.html),
[Asset-Aware Scheduling — Airflow 3.3](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/asset-scheduling.html),
[Timetables (AssetOrTimeSchedule) — Airflow 3.3](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/timetable.html),
[Event-driven scheduling — Astronomer](https://www.astronomer.io/docs/learn/airflow-event-driven-scheduling)
