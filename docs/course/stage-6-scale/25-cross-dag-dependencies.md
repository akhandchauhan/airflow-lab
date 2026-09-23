# Session 25 · Cross-DAG Dependencies

**Goal:** make one DAG depend on another *correctly* — either by imperatively kicking it off (`TriggerDagRunOperator`) or by waiting for it to finish (`ExternalTaskSensor`) — and, crucially, learn *when to use neither* and reach for Assets (Session 09) instead. The one idea to get right: these two operators wire DAGs together by **name and by logical date**, which is powerful and fragile in equal measure. The single most common cross-DAG bug in production is a **logical-date mismatch** in `ExternalTaskSensor` — a sensor that waits forever for a run that, from its point of view, does not exist. Understand the date-matching mechanism and you avoid the trap that eats everyone once.

---

## 1. The problem: a DAG that needs another DAG

You have two pipelines. `ingest` loads last night's Stack Overflow snapshot; `report` builds the dashboard off it. `report` must not start until `ingest` has finished. There are three ways to express that, and choosing wrong is the whole lesson:

| Approach | Direction | Who names whom | Shape |
|---|---|---|---|
| **`TriggerDagRunOperator`** | **push** | producer names the consumer | `ingest` *fires* `report` when it's done |
| **`ExternalTaskSensor`** | **pull** | consumer names the producer | `report` *waits* for `ingest` to finish |
| **Assets** (Session 09) | **decoupled** | neither names the other | `ingest` rings a bell, `report` listens |

**The running analogy:** think of it as coordinating a **kitchen and a waiter**. `TriggerDagRunOperator` is the **kitchen shouting "order up!"** and physically pushing the plate to a specific waiter — the kitchen has to know which waiter. `ExternalTaskSensor` is the **waiter standing at the pass, watching the kitchen**, refusing to leave until *that specific dish* is plated — the waiter has to know the kitchen's schedule. Assets are a **service bell**: the kitchen rings it, whichever waiters care are listening, and neither has to know the other exists. Keep this picture; §6 is about knowing which one the situation actually calls for.

---

## 2. `TriggerDagRunOperator` — imperatively fire another DAG

The push model. A task in the producer DAG **creates a run of another DAG on demand.** Import (Airflow 3 moved the standard operators into their own provider):

```python
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
```

> **Why a provider import?** In Airflow 3 the everyday operators (`BashOperator`, `PythonOperator`, `TriggerDagRunOperator`, `ExternalTaskSensor`, `FileSensor`, …) were extracted from core into the **`apache-airflow-providers-standard`** package. It ships with a standard install, but the import path is `airflow.providers.standard.*`, *not* the old `airflow.operators.*`.

The parameters that matter:

```python
trigger = TriggerDagRunOperator(
    task_id="fire_report",
    trigger_dag_id="s25_report",                     # ← THE MECHANIC: which DAG to run
    conf={"snapshot": "2026-01-01", "rows": 4213},   # payload handed to the run
    wait_for_completion=False,                        # return immediately, or block?
    poke_interval=60,                                 # if waiting, how often to check
    reset_dag_run=True,                               # clear an existing run for that date
    deferrable=False,                                 # free the worker slot while waiting
)
```

- **`trigger_dag_id`** — the `dag_id` to launch. This is the hard coupling: the producer must know the consumer's name.
- **`conf`** — a JSON-serializable dict passed into the triggered run. The triggered DAG reads it via `context["dag_run"].conf` / `params`. This is how you hand *parameters* across the boundary (which asset events, §6, cannot do as flexibly).
- **`wait_for_completion`** — default **`False`**: fire-and-forget, the task succeeds the moment the run is created. Set `True` and the task **blocks until the triggered run reaches a terminal state**, turning "trigger" into "trigger and wait."
- **`poke_interval`** — default **60** seconds; how often it checks the triggered run's status *when* `wait_for_completion=True`.
- **`allowed_states` / `failed_states`** — when waiting, which triggered-run states count as success vs failure (default success = `["success"]`).
- **`reset_dag_run`** — default **`False`**. If a run already exists for the target logical date, triggering again raises a duplicate error. Set `True` to clear and re-create it — essential for **reruns/backfills**, or your second attempt fails on "run already exists."
- **`skip_when_already_exists`** — mark the task **SKIPPED** (instead of erroring) if a run for that logical date already exists.
- **`deferrable`** — when `True` *and* `wait_for_completion=True`, the wait happens on the **triggerer** and the worker slot is released (same slot-saving idea as Session 19's deferrable sensors).

**The subtlety:** `wait_for_completion=False` means downstream tasks in the producer run *immediately* — they do **not** wait for the triggered DAG. If your producer has logic that depends on the report being finished, you must set `wait_for_completion=True`, and then you should ask whether a sensor (or an asset) wouldn't be cleaner than one DAG blocking a slot on another.

---

## 3. `ExternalTaskSensor` — wait for another DAG/task to finish

The pull model. A task in the consumer DAG **blocks until a task (or whole DAG) in another DAG has reached an allowed state.** Import:

```python
from airflow.providers.standard.sensors.external_task import ExternalTaskSensor
```

```python
wait_for_ingest = ExternalTaskSensor(
    task_id="wait_for_ingest",
    external_dag_id="s25_ingest",                 # which DAG to watch
    external_task_id="load_snapshot",             # which task; omit → wait for whole DAG
    allowed_states=["success"],                   # states that satisfy the wait
    failed_states=["failed"],                     # states that FAIL the sensor fast
    execution_delta=None,                         # ← THE MECHANIC: date matching (see §4)
    check_existence=True,                          # fail fast if dag/task doesn't exist
    mode="reschedule",                            # or deferrable=True — don't hold a slot
    poke_interval=60,
)
```

- **`external_dag_id`** — the DAG being watched (required).
- **`external_task_id`** — the specific task to wait on. **Omit it** (leave `None`) to wait for the *entire external DAG run* to succeed. Also `external_task_ids` (a list) and `external_task_group_id` for a task group.
- **`allowed_states`** — states that satisfy the sensor; default `["success"]`.
- **`failed_states`** — states that make the sensor **fail immediately** instead of timing out. **Always set this to `["failed"]`** — otherwise, if the external task fails, your sensor pokes uselessly until `timeout`, hiding the real failure for hours.
- **`skipped_states`** — states that mark *this* sensor as skipped.
- **`check_existence`** — validate the external DAG/task exists before waiting; catches typos in `external_dag_id` immediately rather than after a long timeout.
- **`execution_delta` / `execution_date_fn`** — the date-matching controls. This is the trap; §4.

Run it in **`mode="reschedule"`** or **`deferrable=True`** so it releases the worker slot between checks (Session 19) — a plain-poke sensor holding a slot for hours is itself a 2am incident.

---

## 4. The logical-date matching trap (read this twice)

Here is the mechanism nobody explains until it burns them. `ExternalTaskSensor` does **not** wait for "the latest run of the other DAG." By default it waits for a run of the external DAG **whose logical date is *identical* to the current DAG's logical date.** It matches on the timestamp, not on "most recent."

This is invisible when both DAGs share a schedule and fire at the same logical date. It **breaks the instant their schedules differ**:

- `ingest` runs `@daily` at logical date `2026-01-01T00:00:00`.
- `report` runs `@daily` but at `2026-01-01T06:00:00` (a different schedule).
- `report`'s sensor looks for an `ingest` run at `...T06:00:00` — **which does not exist.** `ingest`'s run is at `...T00:00:00`. The sensor pokes forever and times out, even though `ingest` finished cleanly hours ago.

You reconcile the two dates with one of two parameters — **use exactly one:**

- **`execution_delta`** — a `timedelta` **subtracted** from the current logical date to find the one to look for. If `report` (at 06:00) needs `ingest` (at 00:00 of the same day): `execution_delta=timedelta(hours=6)`. Note the sign: it *subtracts*, so a delta of +6h looks 6 hours *earlier*.
- **`execution_date_fn`** — a callable that receives the current logical date and **returns the logical date(s)** to wait for. Use this when the relationship isn't a fixed offset (e.g. an hourly DAG waiting on the *day's* daily run — you'd truncate to midnight).

```python
from datetime import timedelta

# report @ 06:00 waits for ingest @ 00:00 of the same day:
execution_delta=timedelta(hours=6)     # subtracts 6h from report's logical date
```

> **The rule:** `ExternalTaskSensor` matches on logical date, not recency. **Same schedule → no delta needed. Different schedules → you MUST set `execution_delta` or `execution_date_fn`, or the sensor waits for a run that never existed.** This single fact is the source of ~90% of "my sensor hangs forever" tickets.

---

## 5. Complete runnable reference DAG

Two DAGs, plain TaskFlow — **no BigQuery** (this session teaches the *wiring* between DAGs, not a warehouse call; flagged per R16). `ingest` loads a snapshot on a schedule and fires `report` when done; `report` also carries an `ExternalTaskSensor` to show the pull side. Both patterns in one runnable file so you can see push and pull side by side.

```python
# dags/stage-6-scale/s25/cross_dag_demo.py
from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow.sdk import dag, task
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.standard.sensors.external_task import ExternalTaskSensor


@dag(
    dag_id="s25_ingest",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule="@daily",
    catchup=False,
    tags=["session-25", "cross-dag"],
    default_args={"owner": "akhand", "retries": 1},
)
def ingest():

    @task
    def load_snapshot() -> None:
        print("loaded the stackoverflow snapshot")

    # push model: fire the report DAG the moment ingest finishes
    fire_report = TriggerDagRunOperator(
        task_id="fire_report",
        trigger_dag_id="s25_report",
        conf={"source": "s25_ingest"},          # payload the report can read from dag_run.conf
        wait_for_completion=False,              # fire-and-forget
        reset_dag_run=True,                     # reruns don't blow up on "run exists"
    )

    load_snapshot() >> fire_report


@dag(
    dag_id="s25_report",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule="@daily",
    catchup=False,
    tags=["session-25", "cross-dag"],
    default_args={"owner": "akhand", "retries": 1},
)
def report():

    # pull model: same schedule as ingest, so NO execution_delta is needed
    wait_for_ingest = ExternalTaskSensor(
        task_id="wait_for_ingest",
        external_dag_id="s25_ingest",
        external_task_id="load_snapshot",
        allowed_states=["success"],
        failed_states=["failed"],              # fail fast instead of hanging to timeout
        execution_delta=timedelta(0),          # identical logical date → zero offset
        check_existence=True,
        mode="reschedule",                     # release the worker slot between pokes
        poke_interval=30,
    )

    @task
    def build_dashboard(**context) -> None:
        conf = context["dag_run"].conf or {}
        print(f"building dashboard (triggered by {conf.get('source', 'schedule')})")

    wait_for_ingest >> build_dashboard()


ingest()
report()
```

```bash
python dags/stage-6-scale/s25/cross_dag_demo.py
airflow dags test s25_ingest 2026-01-01
```

In a running scheduler, `s25_ingest` finishing triggers `s25_report` via `fire_report`; the sensor demonstrates the pull side against the same logical date. (`dags test` runs one DAG in isolation — use the scheduler/UI to watch the trigger actually fire the second DAG, and keep `s25_report` unpaused.)

---

## 6. When to prefer Assets over both

Both operators create a **named, dated coupling** between DAGs — and that coupling is exactly what hurts at scale. `TriggerDagRunOperator` forces the producer to hard-code every consumer; add a third downstream DAG and you edit the producer. `ExternalTaskSensor` forces the consumer to know the producer's *dag_id, task_id, and schedule* — and to get the logical-date math right. Both point a dependency **the wrong way** (or make it brittle), and both were the "pre-asset pain" table back in Session 09.

**Assets (Session 09) remove both problems:** the producer declares `outlets=[questions]`, the consumer sets `schedule=[questions]`, and **neither names the other.** No dag_id coupling, no logical-date matching, no held slot, no fan-out edited into the producer. Airflow connects them by the asset's URI.

| Use… | when… |
|---|---|
| **Assets** | the dependency is "run when *this data* is ready" — the **default** for data pipelines. Decoupled, no date math, no held slots. |
| **`TriggerDagRunOperator`** | you need **imperative control** — trigger conditionally inside branching logic, pass rich per-run `conf`, or trigger the *same* DAG with different parameters. Assets can't pass arbitrary params or fire conditionally mid-DAG. |
| **`ExternalTaskSensor`** | you must wait on a **specific task** inside another DAG (not the whole DAG's data output), or wait on a DAG **you don't own / can't modify** to emit an asset. |

The honest default: **reach for Assets first.** Use `TriggerDagRunOperator` when you need to pass parameters or trigger conditionally; use `ExternalTaskSensor` when you can't change the upstream DAG to emit an asset. If you find yourself hand-writing logical-date deltas, that's usually the signal you wanted an asset.

---

## 7. Build spec — your challenge (no solution)

**File:** `dags/stage-6-scale/s25/cross_dag_chain.py` (scaffold: `s25_assignment.py`) · **dag_ids:** `s25_upstream`, `s25_downstream`

Build a two-DAG chain where the schedules **deliberately differ**, forcing you to get the date matching right.

**The problem:**

- **`s25_upstream`** runs `@daily` at midnight and has a task `prepare_data` that logs a message.
- **`s25_downstream`** runs on a *different* schedule (e.g. `0 6 * * *` — daily at 06:00) and must not start until *that same day's* `prepare_data` has succeeded.
- Wire the wait with an **`ExternalTaskSensor`** using the correct **`execution_delta`** (or `execution_date_fn`) so it matches the midnight run from a 06:00 logical date.
- Additionally, have `s25_upstream` **also** trigger a third, ad-hoc run of `s25_downstream` via **`TriggerDagRunOperator`** passing a `conf` flag the downstream logs.

**Constraints:**

- Plain TaskFlow, no BigQuery.
- The sensor must run in `mode="reschedule"` or `deferrable=True` (no slot-holding poke) and set `failed_states=["failed"]`.
- Standard-provider imports (`airflow.providers.standard.*`).
- All DAGs pass the integrity gates: non-empty `tags`, real `owner`, `retries >= 1`.

**Acceptance criteria:**

- `python dags/stage-6-scale/s25/cross_dag_chain.py` parses both DAGs.
- With both unpaused, the 06:00 `s25_downstream` run's sensor **succeeds** against the midnight `s25_upstream` run (proving the `execution_delta` is right), and does **not** hang to timeout.
- The `TriggerDagRunOperator` path creates an extra `s25_downstream` run that logs the `conf` value.
- `python -m pytest tests/ -v` stays green.

**One nudge (only if stuck):** `execution_delta` is *subtracted* from the current logical date. Downstream fires at 06:00 and needs upstream's 00:00 run of the **same day**, so the delta is the difference between those two clock times — a positive `timedelta`, not negative.

---

## 8. Production tip — the 2am page: the sensor that waited for a ghost

The bug that pages you: someone changed `ingest` from `@daily` to run at 02:00 instead of midnight, "to give the upstream feed more time." Nothing in `report` was touched. Next morning, `report` is stuck — its `ExternalTaskSensor` has been poking since 06:00 and will keep poking until its `timeout`, then fail. The dashboard is stale, the on-call is baffled because `ingest` **ran perfectly** and its run is right there, green, in the UI. The sensor isn't looking for "the latest ingest" — it's looking for an ingest run at `report`'s *exact* logical date, and after the schedule change that date no longer lines up. It is waiting for a run that, from its arithmetic, never happened.

- **The habit that prevents it:** any time two DAGs are coupled by `ExternalTaskSensor`, treat their **schedules as a shared contract.** Change one schedule and you must revisit the `execution_delta`/`execution_date_fn` on every sensor pointed at it — put a comment on the sensor naming the upstream schedule it assumes.
- **Always set `failed_states` and a real `timeout`.** A sensor with neither turns a fast, loud failure into a silent multi-hour hang. `failed_states=["failed"]` makes an upstream failure fail the sensor *now*; a bounded `timeout` caps the ghost-hunt.
- **When the date math starts feeling clever, stop — that's the asset signal.** If preventing this class of bug means maintaining offset arithmetic between teams' schedules, the coupling is too tight. An `outlets`/`schedule=[asset]` pair (Session 09) has no logical-date matching to get wrong, and a schedule change upstream simply can't create this failure.

---

Sources:
[Cross-DAG Dependencies — Airflow 3.3](https://airflow.apache.org/docs/apache-airflow/stable/howto/operator/external_task_sensor.html),
[ExternalTaskSensor API — providers-standard](https://airflow.apache.org/docs/apache-airflow-providers-standard/stable/_api/airflow/providers/standard/sensors/external_task/index.html),
[TriggerDagRunOperator API — providers-standard](https://airflow.apache.org/docs/apache-airflow-providers-standard/stable/_api/airflow/providers/standard/operators/trigger_dagrun/index.html),
[Upgrading to Airflow 3 — standard provider split](https://airflow.apache.org/docs/apache-airflow/stable/installation/upgrading_to_airflow3.html),
[Asset-Aware Scheduling — Airflow 3.3](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/asset-scheduling.html)
