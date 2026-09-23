# Session 19 · Sensors & deferrable operators

**Goal:** learn how a task **waits for something** — a file, an upstream task in another DAG, an arbitrary condition — without wasting the cluster while it waits. The one idea to nail: a plain sensor in `poke` mode **holds a worker slot the entire time it waits**, which is the 2am scaling trap; `reschedule` mode and, better, **deferrable** operators hand that slot back so hundreds of things can wait at once on almost no resources. Get poke-vs-reschedule-vs-defer straight and you understand why the triggerer process exists. This is a scale-and-harden topic, so the reference DAG is a plain DAG — BigQuery would be bolted on for no reason here (flagged per R16). *(Stage 6 — scale & harden.)*

---

## 1. Why sensors exist, and the running analogy

A pipeline rarely controls everything it depends on. A partner drops a file "sometime after 6am." An upstream DAG owned by another team finishes "usually by 07:00." A table gets a partition "when the load job is done." You cannot schedule against *when* — you can only schedule against *when it's actually ready*. A **sensor** is a task whose whole job is to **wait until a condition is true, then succeed** so the rest of the DAG can proceed.

**The running analogy for the whole session: waiting for a package.** Three ways to wait:

- **Poke mode** = you stand at the front door and keep opening it every 30 seconds to check. You are physically stuck at the door — you can do nothing else. That "you" is a **worker slot**, and it is occupied the entire wait.
- **Reschedule mode** = you go back to your desk, set a 30-second timer, and only walk to the door when it rings. Between checks you free up the desk (the slot) for someone else, but each check still costs a full walk to the door (a re-scheduled task instance).
- **Deferrable** = you give the courier your phone number and go do real work. When the package lands, *they* call *you*. No standing, no timer, no walking — one lightweight process (the **triggerer**) watches thousands of "phone numbers" at once. This is the endgame, and §6–7 are about it.

Every scaling problem with waiting comes back to this picture: how much of the worker do you tie up while nothing is happening.

---

## 2. What a sensor actually is

A sensor is an operator that subclasses `BaseSensorOperator` and implements one method, `poke(context)`, which returns a **boolean**: `True` means "condition met, stop waiting, succeed," `False` means "not yet, check again later." Airflow calls `poke` on a loop governed by a few parameters.

```python
from airflow.sdk import BaseSensorOperator   # ← the public API base class in Airflow 3
```

| Parameter | What it controls | Default |
|---|---|---|
| `poke_interval` | seconds between checks | 60 |
| `timeout` | max seconds to wait before the sensor **fails** | 7 days (604800) |
| `mode` | `"poke"` or `"reschedule"` — how the wait holds resources | `"poke"` |
| `exponential_backoff` | if `True`, the gap between checks grows exponentially | `False` |
| `max_wait` | upper bound (seconds) on the gap when backoff is on | `None` |
| `soft_fail` | on timeout, mark the task `SKIPPED` instead of `FAILED` | `False` |

The mechanism to internalise: **the sensor's success gate is `poke() == True`.** Everything else (mode, interval, timeout) is only about *how it waits between calls* and *when it gives up* — not about what "done" means.

---

## 3. Poke vs reschedule — the slot cost

This is the single most important trade-off in the session, so read it as a mechanism, not a setting.

| | `poke` (default) | `reschedule` |
|---|---|---|
| Worker slot between checks | **held the whole time** | **released** — task goes back to `up_for_reschedule` |
| State between checks | in-memory (process stays alive) | persisted; task instance re-runs on schedule |
| Best when | waits are **short** (seconds to a few minutes) | waits are **long** (many minutes to hours) |
| The cost | one idle wait = one dead worker slot | each check re-creates a task instance (scheduler + DB overhead) |

Why it matters: `parallelism` (Session 21) is a hard ceiling on concurrent task instances. If 200 DAGs each start a `poke`-mode sensor that waits two hours, you have **200 worker slots doing nothing but re-checking** — and every real task in the cluster queues behind them. That is the classic "the scheduler looks healthy but nothing runs" incident. `reschedule` fixes the slot problem for long waits but pays a re-scheduling tax on every poke and can't poke faster than roughly the scheduler loop.

```python
FileSensor(task_id="wait_file", filepath="/data/drop.csv", mode="reschedule",  # ← frees the slot
           poke_interval=300, timeout=6 * 3600)
```

Rule of thumb: **short wait → `poke`; long wait → `reschedule`; either way, prefer a deferrable version if one exists (§6).**

---

## 4. The two sensors you'll actually use

Both live in the **standard provider** (`apache-airflow-providers-standard`) in Airflow 3 — the sensor classes were moved out of core into that provider, so pin it in `requirements`.

**`FileSensor`** — waits for a file (or glob) to appear on a filesystem the worker can see.

```python
from airflow.providers.standard.sensors.filesystem import FileSensor

wait_drop = FileSensor(
    task_id="wait_drop",
    filepath="/data/stackoverflow/questions_*.csv",   # what to wait for
    fs_conn_id="fs_default",
    mode="reschedule",
    poke_interval=300,
    timeout=6 * 3600,
)
```

**`ExternalTaskSensor`** — waits for a task (or whole DAG) in **another DAG** to succeed for the matching logical date. This is how you couple two teams' DAGs without one importing the other.

```python
from airflow.providers.standard.sensors.external_task import ExternalTaskSensor

wait_loader = ExternalTaskSensor(
    task_id="wait_loader",
    external_dag_id="s12_producer",       # the DAG we depend on
    external_task_id="load_questions",    # None = wait for the whole DAG
    allowed_states=["success"],
    mode="reschedule",
    timeout=3 * 3600,
)
```

The subtle trap with `ExternalTaskSensor`: it matches on **logical date**. If the two DAGs run on different schedules, you must line the dates up with `execution_date_fn` or `execution_delta`, or the sensor waits for a run that never had that exact timestamp. (Contrast this with assets in §8 — assets remove the date-matching problem entirely.)

---

## 5. `@task.sensor` — a sensor from a plain function

For a custom condition you don't need a whole operator class — decorate a function with `@task.sensor`. The function **is** the `poke`: return a truthy/falsy value, or a `PokeReturnValue` when you also want to push an XCom.

```python
from airflow.sdk import PokeReturnValue, task   # ← both are public airflow.sdk names

@task.sensor(poke_interval=60, timeout=3600, mode="reschedule")   # ← THE MECHANIC: function = poke()
def wait_row_count() -> PokeReturnValue:
    rows = check_partition()                     # your own check; must be defined in this snippet's DAG
    if rows == 0:
        return PokeReturnValue(is_done=False)    # not ready → poke again
    return PokeReturnValue(is_done=True, xcom_value=rows)   # ready → succeed AND push `rows` as XCom
```

- Return **`True`/`False`** for the simple case (no XCom), or a **`PokeReturnValue(is_done=..., xcom_value=...)`** to succeed *and* hand a value downstream in one step.
- `PokeReturnValue.is_done` is the success gate; `xcom_value` is optional.
- All the `BaseSensorOperator` knobs (`mode`, `poke_interval`, `timeout`) are decorator arguments.

This is the recommended way to write a bespoke wait in Airflow 3 — lighter than subclassing, and it drops straight into a TaskFlow DAG.

---

## 6. Deferrable operators — hand the slot to the triggerer

A sensor in `reschedule` mode still *wakes up, checks, sleeps* on a task instance. A **deferrable** operator does something categorically different: it runs just long enough to register **what** it's waiting for, then calls `self.defer(...)`, which **suspends the task and releases the worker completely**. The waiting is taken over by the **triggerer** — a single async process that can watch thousands of pending conditions on one event loop (`asyncio`). When the condition fires, the triggerer wakes the task back up on a worker to finish. This is the "give the courier your number" step from §1.

Why this is the endgame for waiting: **a deferred task holds zero worker slots and zero scheduler slots while it waits.** Ten thousand DAGs can be "waiting" simultaneously and the only cost is memory for ten thousand small trigger coroutines on the triggerer — not ten thousand worker slots. Nothing else in Airflow scales waiting like this.

**Turning it on is usually one flag.** Most standard/provider sensors and operators accept `deferrable=True`:

```python
FileSensor(task_id="wait_drop", filepath="/data/drop.csv", deferrable=True)   # ← THE MECHANIC: defers instead of poking
```

You can flip the whole deployment's default with the config `[operators] default_deferrable = True`, so operators that support it defer unless told otherwise.

**What `deferrable=True` does under the hood** — the operator's `execute` starts, decides it must wait, and calls:

```python
from airflow.triggers.base import BaseTrigger, TriggerEvent   # ← trigger base + the event it yields

self.defer(
    trigger=MyTrigger(...),          # a serialisable async object that does the waiting
    method_name="execute_complete",  # the operator method to resume on when the trigger fires
    timeout=timedelta(hours=6),      # optional deferral timeout
)
```

The pieces:

- **`self.defer(...)`** — the call that suspends the task and ships `trigger` to the triggerer. Available on any operator (`BaseOperator`/`BaseSensorOperator`).
- **`BaseTrigger`** — subclass it to write a custom trigger. It requires `serialize()` (returns `(classpath, kwargs)` so the trigger can be rebuilt in the triggerer) and an **`async def run(self)`** that must `yield` a `TriggerEvent` when the condition is met. `run` must be genuinely async — every blocking call `await`ed — because one event loop hosts all triggers.
- **`TriggerEvent(payload)`** — what `run` yields; its payload is passed to the resume method.
- **`method_name`** — the operator method Airflow calls when the event arrives, typically `execute_complete(self, context, event=None)`, which returns the final result or raises to fail.

You rarely write a `BaseTrigger` by hand — the value is knowing that `deferrable=True` is *this whole machinery* behind one keyword, and that it needs a **triggerer running** (`airflow triggerer`) or deferred tasks just sit there forever.

---

## 7. Sensors vs assets vs deferrable — which "wait" to reach for

These three all answer "run when something is ready," and people conflate them. They are different tools:

| Tool | How it waits | Couples DAGs by | Slot cost while waiting | Reach for it when |
|---|---|---|---|---|
| **Sensor (poke)** | task polls in a held slot | task/DAG id or a condition | **one full slot** | short wait, condition isn't another Airflow task |
| **Sensor (reschedule)** | task polls, slot released between | task/DAG id or a condition | ~zero between checks, tax per check | long wait, no deferrable version exists |
| **Deferrable operator** | triggerer watches async, task suspended | whatever the trigger watches | **~zero** | long/high-fan-out waits; a `deferrable=True` exists |
| **Asset** (Session 12) | producer *rings*, consumer scheduled on it | a shared **asset name**, not dag/task ids | **none** — no waiting task at all | the producer is an Airflow task you can add an outlet to |

The mental model: **if the thing you wait on is another Airflow task you control, use an asset (Session 12) — no polling, no date-matching, nothing waiting.** If it's *outside* Airflow (a file, an API, a partition) or an upstream you can't modify, use a sensor — and make it **deferrable** if a version exists, `reschedule` if not. Reserve plain `poke` for genuinely short waits.

---

## 8. Complete runnable DAG (your reference)

A wait-then-work pipeline: a `@task.sensor` waits for a condition, a `FileSensor` (deferrable) waits for a drop file, then a normal task runs. Plain DAG — this topic is about *how tasks wait*, so a BigQuery call would add cost and noise for no teaching value (flagged per R16).

```python
from __future__ import annotations

import pendulum
from airflow.providers.standard.sensors.filesystem import FileSensor
from airflow.sdk import PokeReturnValue, dag, task


@dag(
    dag_id="s19_sensors_demo",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["session-19", "sensors"],
    default_args={"owner": "akhand", "retries": 1},
)
def pipeline():

    @task.sensor(poke_interval=30, timeout=600, mode="reschedule")   # function == poke(); frees slot
    def wait_ready() -> PokeReturnValue:
        ready = True                                   # your real check goes here
        return PokeReturnValue(is_done=ready, xcom_value={"checked": True})

    wait_file = FileSensor(                             # deferrable: hands the wait to the triggerer
        task_id="wait_file",
        filepath="/tmp/stackoverflow_drop.csv",
        deferrable=True,
        poke_interval=60,
        timeout=3600,
    )

    @task
    def process() -> None:
        print("both waits cleared → processing")

    [wait_ready(), wait_file] >> process()


pipeline()
```

```bash
python dags/s19/s19_examples.py
airflow dags test s19_sensors_demo 2026-01-01
```

`dags test` runs the sensors in-process. `wait_ready` returns `is_done=True` on the first poke and succeeds immediately; `wait_file` will wait for `/tmp/stackoverflow_drop.csv` — `touch` it in another terminal (or point `filepath` at a file that exists) to let the run finish. To see the deferral for real, run a **triggerer** (`airflow triggerer`) and trigger the DAG from the scheduler/UI — the `wait_file` task will show state `deferred`, holding no worker slot, until the file lands.

---

## 9. Build spec — your challenge (no solution)

**File:** `dags/s19/s19_assignment.py` · **dag_id:** `s19_assignment`

Build a **cross-DAG wait** that consumes the Session 12 producer without polling a held slot.

**The problem:**

- One `ExternalTaskSensor` waits for the task `load_questions` in DAG `s12_producer` to succeed — running in **`reschedule`** mode (or `deferrable=True`) so it never holds a worker slot while waiting.
- One `@task.sensor` that returns a `PokeReturnValue` — `is_done=False` until your condition holds, then `is_done=True` with an `xcom_value`.
- A final `@task` that runs only after both waits clear and prints the `xcom_value` the sensor pushed.

**Constraints:**

- Plain TaskFlow, no BigQuery.
- Standard-provider imports (`airflow.providers.standard.sensors.external_task`, `...filesystem`), `airflow.sdk` for `dag`/`task`/`PokeReturnValue`.
- No sensor may run in plain `poke` mode — justify `reschedule` vs `deferrable` in a comment.
- Passes the integrity gates: `tags`, real `owner`, `retries >= 1`.

**Acceptance criteria:**

- `python dags/s19/s19_assignment.py` parses (prints nothing).
- With `s12_producer` present, `airflow dags test s19_assignment 2026-01-01` reaches the final task once the external task's run is found.
- The `ExternalTaskSensor` task shows `up_for_reschedule` (or `deferred`), **never a long-held running slot**.
- `python -m pytest tests/ -v` stays green.

**One nudge (only if stuck):** if the two DAGs run on different schedules, `ExternalTaskSensor` won't find a matching logical date — look at `execution_delta` / `execution_date_fn`. And if you reach for `deferrable=True`, remember a **triggerer must be running** or the task defers forever.

---

## 10. Production tip — the poke-mode sensor that ate the cluster

The bug that pages you at 2am: a partner's file was late one night, and the DAG that waits for it uses a `FileSensor` in **default `poke` mode** with a 12-hour `timeout`. Forty other DAGs use the same pattern. The file didn't come, so forty sensors sat in `poke` mode, each **holding a worker slot and doing nothing but re-checking**. `parallelism` was 64. Real tasks — loads, reports, the alerting DAG itself — queued behind the forty idle waiters and never ran. The dashboards showed the scheduler healthy and workers "busy," which is exactly why it took an hour to find: the workers *were* busy, busy waiting.

- **Never leave a long wait in `poke` mode.** The default is `poke` and the default `timeout` is *seven days* — that combination is a slot leak waiting for a slow upstream. For anything that can wait more than a couple of minutes, use `deferrable=True` (best) or `mode="reschedule"` (if no deferrable version exists).
- **Set a real `timeout`.** A wait with no ceiling is a wait that pins a slot until someone notices. Bound it to slightly more than the worst plausible arrival, and use `soft_fail=True` if a no-show should skip rather than page.
- **If you're waiting on your own upstream, don't wait at all** — an asset (Session 12) means zero polling tasks and no date-matching. The best sensor is the one you deleted.

---

## 11. Verify + commit

```bash
python dags/s19/s19_assignment.py
airflow dags test s19_assignment 2026-01-01
python -m pytest tests/ -v
git add -A && git commit -m "session 19: sensors & deferrable operators" && git push
```

Done when the external wait clears without holding a running slot and the final task prints the sensor's `xcom_value`. Tick Session 19 in `docs/course/README.md`.

**Pre-push habit:** `ruff check dags/ include/ tests/ --select E,F,AIR3 && python -m pytest tests/ -v`.

Sources:
[Sensors — Airflow 3.3](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/sensors.html),
[Deferrable Operators & Triggers — Airflow 3.3](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/deferring.html),
[airflow.sdk API Reference (Task SDK)](https://airflow.apache.org/docs/task-sdk/stable/api.html),
[FileSensor — standard provider](https://airflow.apache.org/docs/apache-airflow-providers-standard/stable/_api/airflow/providers/standard/sensors/filesystem/index.html),
[Cross-DAG dependencies (ExternalTaskSensor) — standard provider](https://airflow.apache.org/docs/apache-airflow-providers-standard/stable/sensors/external_task_sensor.html)
