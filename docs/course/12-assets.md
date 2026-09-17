# Session 12 · Assets (data-aware scheduling), end to end

**Goal:** stop scheduling on a **clock that hopes** the data is ready, and start
triggering on the data **actually being updated**. Understand *why* assets exist (the
problem with cron/sensors/`TriggerDagRunOperator`), the core producer→consumer model,
and the full ladder up to conditional scheduling, metadata, aliases, and event-driven
runs. Plain DAGs, no BigQuery. *(Stage 3 — react to data, not the clock.)*

---

## 1. Why we needed assets

Everything before this session scheduled on **time**. That has a hole: a `@daily`
report runs at midnight and just *hopes* last night's load finished. When the load is
late, the report runs on stale or empty data — the exact 2am incident from Session 04.

The pre-asset ways to fix that, and why each hurts:

| Approach | What it does | The pain |
|---|---|---|
| **Pad the schedule** | run the report "late enough" (03:00 not 00:00) | guesswork; still breaks when the load is *later*; wasted hours |
| **Sensor** (`ExternalTaskSensor`, `FileSensor`) | the consumer *polls* until upstream is done | burns a worker slot polling; couples DAGs by task ids; brittle |
| **`TriggerDagRunOperator`** | producer *imperatively* kicks the consumer | producer must know every consumer; hard-codes the fan-out; reverse of how deps should point |

**An Asset flips it to declarative.** The consumer just names the **data** it depends
on; the producer just says "I update this data." Neither references the other's DAG.
Airflow watches the asset and runs the consumer the moment the data is fresh.

```
TIME-BASED                          ASSET-BASED
schedule="@daily"                   schedule=[questions_asset]
→ runs at 00:00, hopes data ready   → runs WHEN questions is actually updated
```

This is where the platform is heading: pipelines become a graph of **data
dependencies**, not a wall of cron lines.

---

## 2. The core model — producer → asset → consumer

An **Asset** is a named handle for a piece of data (a table, a file, an object).

```python
from airflow.sdk import Asset

questions = Asset(uri="file:///data/questions.csv", name="questions")
```

Two halves wire around it:

```
Producer DAG                     Asset "questions"            Consumer DAG
────────────                     ─────────────────            ────────────
@task(outlets=[questions])  ──▶  marked UPDATED   ──▶  @dag(schedule=[questions]) runs
   (on task SUCCESS)             (an asset event)        (no cron, fires on the event)
```

The one rule that makes it safe: **an asset is marked updated only if the producing
task completes successfully.** A failed load produces no event, so downstream never
runs on bad data.

- `uri` identifies the data (RFC-3986; the `airflow://` scheme is reserved — use an
  `x-` prefix for custom schemes). `name` is the friendly handle in the UI's **Assets**
  view.

---

## 3. Producing an asset

Two ways. Add `outlets` to any task, **or** use the `@asset` decorator.

```python
from airflow.sdk import task, Asset

questions = Asset(uri="file:///data/questions.csv", name="questions")

@task(outlets=[questions])          # ← THE MECHANIC: this task updates `questions` on success
def load_questions() -> None:
    print("loaded questions")
```

The **`@asset` decorator** is shorthand — it creates the `Asset`, a `DAG`, and a task
that outlets it, all at once:

```python
from airflow.sdk import asset

@asset(uri="file:///data/questions.csv", schedule="@daily")
def questions() -> None:            # this function IS the producer DAG for the asset
    print("loaded questions")
```

Use `@task(outlets=...)` when the producer is part of a bigger pipeline; use `@asset`
when the whole DAG's job is to produce that one dataset.

---

## 4. Consuming — schedule on the asset

The consumer names the asset in `schedule`. No cron, no sensor, no reference to the
producer's DAG id.

```python
from airflow.sdk import dag, task

@dag(schedule=[questions], ...)     # ← runs whenever `questions` is updated
def report():
    @task
    def build() -> None:
        print("questions updated → building report")
    build()
```

To see **what** triggered the run (and any metadata the producer attached):

```python
@task
def build(**context) -> None:
    events = context["triggering_asset_events"]     # which asset events fired this run
    for asset, evs in events.items():
        print(f"triggered by {asset.name}, {len(evs)} event(s)")
```

---

## 5. Conditional scheduling — waiting on several assets

Real reports need *several* inputs ready. Combine assets with boolean operators:

```python
@dag(schedule=(questions & answers), ...)   # AssetAll — run when BOTH updated
@dag(schedule=(questions | answers), ...)   # AssetAny — run when EITHER updates
```

- `&` = `AssetAll` (AND), `|` = `AssetAny` (OR); you can nest: `(a & b) | c`.
- Mix time **and** data with `AssetOrTimeSchedule` — "run when the asset updates, but
  also at least daily as a floor."

This is the payoff: a mart that needs `questions` **and** `answers` fresh runs exactly
once both landed — never early, never on a padded timer.

---

## 6. The advanced ladder

Once the basics click, four features cover the hard cases:

**a) Metadata on an event** — tell consumers *what* changed (row counts, a path):

```python
from airflow.sdk import Metadata

@asset(schedule=None)
def questions(self):
    yield Metadata(self, {"row_count": 4213})       # attach extra (JSON-serializable)
# or inside a @task:  context["outlet_events"][questions].extra = {"row_count": 4213}
```

Consumers read it from `triggering_asset_events` — e.g. skip if `row_count == 0`.

**b) `AssetAlias`** — the asset's identity isn't known until runtime (dynamic path):

```python
from airflow.sdk import AssetAlias

@task(outlets=[AssetAlias("daily-exports")])
def export(*, outlet_events) -> None:
    path = compute_path()                           # decided at run time
    outlet_events[AssetAlias("daily-exports")].add(Asset(f"file://{path}"))
```

Consumers schedule on the **alias**; whatever concrete asset the run resolves to
triggers them.

**c) `@asset.multi`** — one task that updates several assets at once.

**d) Event-driven watchers** — trigger from an **external** event (a queue message),
not another Airflow task, via `AssetWatcher`. That's Session 14; know it exists.

---

## 7. Complete runnable DAGs (your reference)

Producer + consumer sharing one asset. Plain, no BigQuery.

```python
from __future__ import annotations

import pendulum
from airflow.sdk import Asset, dag, task

# the shared data dependency — defined once, imported by both DAGs
questions = Asset(uri="file:///data/questions.csv", name="questions")


@dag(
    dag_id="s12_producer",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule="@daily",                       # producer runs on a clock…
    catchup=False,
    tags=["session-12", "assets"],
    default_args={"owner": "akhand", "retries": 1},
)
def producer():

    @task(outlets=[questions])               # …and updates the asset on success
    def load_questions() -> None:
        print("loaded questions → asset 'questions' updated")

    load_questions()


@dag(
    dag_id="s12_consumer",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=[questions],                    # consumer runs when the asset updates
    catchup=False,
    tags=["session-12", "assets"],
    default_args={"owner": "akhand", "retries": 1},
)
def consumer():

    @task
    def build_report(**context) -> None:
        events = context["triggering_asset_events"]
        print(f"questions is fresh ({len(events)} asset event) → building report")

    build_report()


producer()
consumer()
```

```bash
python dags/s12/s12_examples.py               # parses both DAGs
airflow dags test s12_producer 2026-01-01     # runs the producer; updates the asset
```

In a running scheduler, the moment `s12_producer` succeeds, `s12_consumer` starts on
its own — check the **Assets** view in the UI to see the event. (`dags test` runs one
DAG in isolation, so trigger the producer through the scheduler/UI to watch the
consumer fire, or `airflow dags test s12_consumer 2026-01-01` to test its body alone.)

---

## 8. Build spec — your challenge (no solution)

**File:** `dags/s12/s12_assignment.py`  ·  **dag_ids:** `s12_prod_a`, `s12_prod_b`, `s12_mart`

Build a **two-input mart** that runs only when both sources are fresh.

**The problem:**

- Define **two** assets, e.g. `orders` and `refunds`.
- **Two producer DAGs**, each with a task that `outlets` one asset and logs a message.
- **One consumer DAG** scheduled on `(orders & refunds)` — it runs only when **both**
  have updated — whose task logs which asset events triggered it
  (`triggering_asset_events`).

**Constraints:**

- Plain TaskFlow, no BigQuery.
- The consumer references the **assets**, never the producers' dag_ids.
- All DAGs pass the integrity gates: `tags`, real `owner`, `retries >= 1`.

**Acceptance criteria:**

- `python dags/s12/s12_assignment.py` parses all three DAGs.
- The UI **Assets** view shows both assets, each with its producer and the mart as
  consumer.
- Running both producers (in the scheduler) triggers the mart exactly once; running
  only one does **not** trigger it.
- `python -m pytest tests/ -v` stays green.

---

## 9. Production tip — an asset is a contract, keep it honest

- **Only success updates an asset — lean on that.** Don't emit the asset event from a
  task that "mostly" worked; let the update *be* the signal that the data is complete.
  A consumer trusts "updated" to mean "safe to read."
- **Attach a row count and gate on it.** Producers should stamp `extra={"row_count":
  n}`; consumers read it from `triggering_asset_events` and short-circuit on `0`. That
  turns "the table changed" into "the table changed *and* has data" — the difference
  between a real trigger and the empty-table incident.
- **Name assets by the data, stably.** The `name`/`uri` is the contract other teams
  schedule on; renaming it silently breaks every downstream. Treat it like a public
  API.

---

## 10. Verify + commit

```bash
python dags/s12/s12_assignment.py
airflow dags test s12_prod_a 2026-01-01
python -m pytest tests/ -v
git add -A && git commit -m "session 12: assets end to end" && git push
```

Done when the Assets view shows the producer→asset→consumer chain and the mart fires
only when both inputs are fresh. Tick Session 12 in `docs/course/README.md`.

**Pre-push habit:** `ruff check dags/ include/ tests/ --select E,F,AIR3 && python -m pytest tests/ -v`.

Sources:
[Assets & data-aware scheduling — Airflow 3.3](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/assets.html),
[Asset-aware scheduling — Airflow 3.3](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/asset-scheduling.html)
