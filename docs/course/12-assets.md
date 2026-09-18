# Session 12 · Assets (data-aware scheduling), end to end

**Goal:** stop scheduling on a **clock that hopes** the data is ready, and start
running when the job that produces the data **actually finished**. The one idea to get
right: an asset is **not** the data and Airflow does **not** watch your files or tables
— an asset is a *name*, and a producer task raising a "done" signal against that name
is what triggers the consumer. Get that, and the rest (conditional scheduling,
metadata, aliases, event-driven) is detail. Plain DAGs, no BigQuery. *(Stage 3 — react
to data, not the clock.)*

---

## 1. Why we needed assets

Everything before this session scheduled on **time**. That has a hole: a `@daily`
report runs at midnight and just *hopes* last night's load finished. When the load is
late, the report runs on stale or empty data — the exact 2am incident from Session 04.

The pre-asset fixes, and why each one hurts:

| Approach | What it does | The pain |
|---|---|---|
| **Pad the schedule** | run the report "late enough" (03:00 not 00:00) | guesswork; still breaks when the load is *later*; wasted hours |
| **Sensor** (`ExternalTaskSensor`, `FileSensor`) | the consumer *polls* until upstream is done | burns a worker slot polling; couples DAGs by task id; brittle |
| **`TriggerDagRunOperator`** | producer *imperatively* kicks the consumer | producer must know every consumer; hard-codes the fan-out; deps point the wrong way |

Notice the shape of all three: the consumer is forced to know *when* or *who*. An asset
removes both questions. The consumer declares **what data** it depends on; the producer
declares **what data** it makes. Neither names the other's DAG. Airflow connects them.

```
TIME-BASED                          ASSET-BASED
schedule="@daily"                   schedule=[questions]
→ fires at 00:00, hopes it's ready  → fires when the loader task SUCCEEDS
```

This is where the platform is heading: pipelines stop being a wall of cron lines and
become a **graph of data dependencies**.

### The whole idea in plain words

Two DAGs. One makes data, one uses it. You want the second to run **right after** the
first finishes — not at a guessed time.

- Old way: the report is set to "run at 3am and hope the loader was done by then."
- Asset way: the report is set to "run **when the loader finishes**." No time at all.

The thing in the middle — the "when the loader finishes" — is the **asset**. Think of
it as a doorbell:

```
loader finishes  ──rings──▶  🔔 "questions"  ──▶  report wakes up and runs
```

The loader **rings the bell** when it's done. The report is **listening** for that
bell. They never talk to each other directly — one rings, one listens, and the bell
(the asset) is the only thing between them. Everything below is just detail on that one
picture.

---

## 2. What an asset actually is (the part everyone gets wrong)

An **Asset** is a **name for a piece of data** plus a **ledger of "done" events** in
Airflow's metadata DB. That's it. It is emphatically **not** the file or table, and
Airflow never opens it.

> **The rule that makes everything else make sense:** Airflow does not monitor your
> data. It monitors **whether a task succeeded**. When a task that lists an asset in its
> `outlets` finishes successfully, Airflow writes one row — an *asset event* — and any
> DAG scheduled on that asset runs. The bytes on disk are irrelevant to the trigger.

**Back to the doorbell.** The producer rings the bell when it finishes; the consumer is
whoever's listening for it. You react to **the ring**, not to walking over and checking
who's at the door. Two consequences fall straight out of that, and both bite people:

- The ring tells you *someone finished a delivery* — not *what they brought*. An asset
  event means the task **succeeded**, not that the data is non-empty or correct. (§10 is
  how you make the ring mean more.)
- If you're **out** (the consumer DAG is **paused**), rings that happen while you're
  gone are **missed** — you come home to silence, not a pile of "you missed 3 rings"
  notes. It waits for the *next* ring.

```python
from airflow.sdk import Asset

questions = Asset(uri="file:///data/questions.csv", name="questions")
```

- `uri` is a **globally unique label**, not a location Airflow reads (RFC-3986; the
  `airflow://` scheme is reserved — use an `x-` prefix for a custom scheme).
- `name` is the friendly handle shown in the UI's **Assets** view.
- Define the asset **once** in a shared module and import it into both the producer and
  the consumer — same Python object, same identity.

---

## 3. Three verbs: outlets, inlets, schedule

Assets attach to tasks and DAGs in exactly three places. Keeping them straight removes
most confusion:

| Where | On | Meaning | Affects scheduling? |
|---|---|---|---|
| `outlets=[asset]` | a **task** | "this task **produces** the asset" → success writes an event | **Yes** — triggers consumers |
| `schedule=[asset]` | a **DAG** | "**run this DAG** when the asset gets an event" | **Yes** — this *is* the trigger |
| `inlets=[asset]` | a **task** | "this task wants to **read** the event's metadata" | **No** — read-only access |

The common mistake: expecting `inlets` to schedule something. It doesn't. `inlets` only
lets a task *see* what came in (`inlet_events`); the trigger is `schedule` + `outlets`.

---

## 4. Producing an asset

Two ways. Add `outlets` to any task, **or** use the `@asset` decorator.

```python
from airflow.sdk import task, Asset

questions = Asset(uri="file:///data/questions.csv", name="questions")

@task(outlets=[questions])          # ← THE MECHANIC: success here writes an asset event
def load_questions() -> None:
    print("loaded questions")
```

The **`@asset` decorator** is shorthand — one decorator creates the `Asset`, a `DAG`,
and a task that outlets it:

```python
from airflow.sdk import asset

@asset(uri="file:///data/questions.csv", schedule="@daily")
def questions() -> None:            # this function IS the producer DAG for the asset
    print("loaded questions")
```

Use `@task(outlets=...)` when the producer is one step of a bigger pipeline; use
`@asset` when a whole DAG's only job is to produce that one dataset.

**Subtlety worth knowing:** the event fires the instant the **first** task with that
outlet succeeds — even if later tasks in the same DAG also touch the asset. Put the
outlet on the task that means "the data is now complete," not an early one.

---

## 5. Consuming — schedule on the asset

The consumer names the asset in `schedule`. No cron, no sensor, no reference to the
producer's DAG id.

```python
from airflow.sdk import dag, task

@dag(schedule=[questions], ...)     # ← runs when `questions` gets an event
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

**The paused-DAG trap, spelled out:** asset events only count toward a consumer's
schedule **while it is unpaused**. Pause the report for maintenance, let three loads run
past, unpause — it does **not** fire three times to catch up. It starts fresh and waits
for the *next* event. (This is why backfilling history stays a time-based job.)

---

## 6. Conditional scheduling — waiting on several assets

Real reports need *several* inputs ready. Combine assets with boolean operators:

```python
@dag(schedule=(questions & answers), ...)   # AssetAll — run when BOTH have an event
@dag(schedule=(questions | answers), ...)   # AssetAny — run when EITHER gets one
```

- `&` = `AssetAll` (AND), `|` = `AssetAny` (OR); nest freely: `(a & b) | c`.
- With multiple assets, Airflow waits for an event from **each**, runs once, then
  **resets** the set and waits for the next full round — so a chatty producer firing
  `orders` ten times won't run the mart ten times before `refunds` arrives.
- Mix time **and** data with `AssetOrTimeSchedule` — "run when the asset updates, and
  also at least once a day as a floor" (so a dead producer doesn't mean a dead report).

This is the payoff: a mart that needs `questions` **and** `answers` fresh runs exactly
once both landed — never early, never on a padded timer.

---

## 7. The advanced ladder

Once the basics click, four features cover the hard cases:

**a) Metadata on an event** — make the ring mean more than "something happened"; tell
consumers *what* changed (row counts, a path):

```python
from airflow.sdk import Metadata

@asset(schedule=None)
def questions(self):
    yield Metadata(self, {"row_count": 4213})       # attach extra (JSON-serializable)
# or inside a @task:  context["outlet_events"][questions].extra = {"row_count": 4213}
```

Consumers read it from `triggering_asset_events` — e.g. skip the run if `row_count == 0`.

**b) `AssetAlias`** — the concrete asset isn't known until runtime (a dynamic path):

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

**d) Event-driven watchers** — the flag is raised by something **outside** Airflow (a
queue message), via `AssetWatcher`. Recall §2: normally Airflow only knows about events
from **tasks, the REST API, or the UI** — a watcher is how you bridge a real external
system. That's Session 14; just know it exists.

---

## 8. Complete runnable DAGs (your reference)

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

    @task(outlets=[questions])               # …and writes an asset event on success
    def load_questions() -> None:
        print("loaded questions → asset event written")

    load_questions()


@dag(
    dag_id="s12_consumer",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=[questions],                    # consumer runs on the event, not a clock
    catchup=False,
    tags=["session-12", "assets"],
    default_args={"owner": "akhand", "retries": 1},
)
def consumer():

    @task
    def build_report(**context) -> None:
        events = context["triggering_asset_events"]
        print(f"triggered by an asset event ({len(events)} asset) → building report")

    build_report()


producer()
consumer()
```

```bash
python dags/s12/s12_examples.py               # parses both DAGs
airflow dags test s12_producer 2026-01-01     # runs the producer; writes the event
```

In a running scheduler, the moment `s12_producer` succeeds, `s12_consumer` starts on
its own — check the **Assets** view in the UI to see the event. (`dags test` runs one
DAG in isolation, so trigger the producer through the scheduler/UI to watch the consumer
fire, or `airflow dags test s12_consumer 2026-01-01` to test its body alone. And make
sure `s12_consumer` is **unpaused** — a paused consumer ignores the event, §5.)

---

## 9. Build spec — your challenge (no solution)

**File:** `dags/s12/s12_assignment.py`  ·  **dag_ids:** `s12_prod_a`, `s12_prod_b`, `s12_mart`

Build a **two-input mart** that runs only when both sources are fresh.

**The problem:**

- Define **two** assets, e.g. `orders` and `refunds`.
- **Two producer DAGs**, each with a task that `outlets` one asset and logs a message.
- **One consumer DAG** scheduled on `(orders & refunds)` — it runs only when **both**
  have an event since its last run — whose task logs which asset events triggered it
  (`triggering_asset_events`).

**Constraints:**

- Plain TaskFlow, no BigQuery.
- The consumer references the **assets**, never the producers' dag_ids.
- All DAGs pass the integrity gates: `tags`, real `owner`, `retries >= 1`.

**Acceptance criteria:**

- `python dags/s12/s12_assignment.py` parses all three DAGs.
- The UI **Assets** view shows both assets, each with its producer and the mart as a
  consumer.
- With `s12_mart` unpaused, running **both** producers triggers it exactly once; running
  only one does **not** trigger it.
- `python -m pytest tests/ -v` stays green.

**One nudge (only if stuck):** `&` combines the two assets into an `AssetAll` condition
— you pass the *expression* `(orders & refunds)` straight to `schedule=`, you don't
write any waiting logic yourself.

---

## 10. Production tip — the ring says "done," not "good"

The bug that pages you at 2am: the loader task hit an empty upstream, wrote **zero
rows**, and *succeeded*. Success wrote the asset event, the event triggered the mart,
and the mart cheerfully published an empty dashboard. Assets did exactly what they
promise — and that's the trap: **an asset event means the task succeeded, not that the
data is any good** (§2, the ring says a delivery happened, not what's inside).

- **Make the ring mean more.** Producers stamp `extra={"row_count": n}`; the consumer
  reads it from `triggering_asset_events` and short-circuits on `0`. That turns "the
  table changed" into "the table changed *and* has rows."
- **Only real completion should ring the bell.** Put `outlets` on the task that means
  the data is *complete and validated*, never a "mostly worked" early step.
- **Treat the name/URI as a public API.** Other teams schedule on it; renaming it
  silently breaks every downstream. Pick it once, keep it stable.

---

## 11. Verify + commit

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
[Asset Definitions — Airflow 3.3](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/assets.html),
[Asset-Aware Scheduling — Airflow 3.3](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/asset-scheduling.html),
[Basic asset-based scheduling — Astronomer](https://www.astronomer.io/docs/learn/airflow-datasets),
[Airflow Assets — Seckin Dinc](https://seckindinc.substack.com/p/airflow-assets),
[Airflow dataset scheduling — Datacoves](https://datacoves.com/post/airflow-schedule)
