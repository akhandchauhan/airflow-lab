# Session 04 · Branching & Trigger Rules

**One line:** this is how a DAG makes **decisions** — take one path or another,
stop early when a check fails, and control **when** a task is allowed to run based
on what happened before it.

Three tools:

| Tool | Plain meaning |
|---|---|
| `@task.branch` | a **fork in the road** — pick which path to take; the other path is skipped |
| `@task.short_circuit` | a **stop sign** — if a check is False, skip everything after it |
| `TriggerRule` | the rule for **when** a task may start (default: only after all its inputs succeed) |

---

## 0. The analogy: a road trip

- **Branch** = a fork in the road. A sign sends you either left or right. The road
  you don't take is closed off (its tasks are **skipped**).
- **Short-circuit** = a "BRIDGE OUT" barrier. If the bridge is out (your check
  returns False), you stop, and everything further down that road is cancelled.
- **Trigger rule** = the rule at a junction for *when you're allowed to go*.
  Normally: "go only when all the roads feeding in are clear." But you can change
  it — for example a cleanup crew that goes in *no matter what happened*.

Keep this picture; every section below maps back to it.

---

## 1. `@task.branch` — pick a path

A branch task is a normal `@task`, but instead of returning data it **returns the
`task_id`** (a string) of the task you want to run next. Every other task directly
below the branch is marked **skipped**.

```python
@task.branch
def choose_path() -> str:
    row_count = 5000
    # return the task_id string of the branch to run; the other is skipped
    return "refresh_full" if row_count > 1000 else "load_incremental"

@task(task_id="refresh_full")
def full_refresh() -> None:
    print("full refresh")

@task(task_id="load_incremental")
def incremental_load() -> None:
    print("incremental load")

branch = choose_path()
branch >> [full_refresh(), incremental_load()]   # branch returns ONE of these task_ids
```

- It returns a **`task_id` string**, not the function. (That's why the task_ids here
  — `"refresh_full"`, `"load_incremental"` — are written out explicitly: the branch
  returns those strings.)
- You can return a **list** of task_ids to run several paths.
- The branch and its choices must be **directly wired** (`branch >> [a, b]`), or
  Airflow can't skip the right ones.

---

## 2. `@task.short_circuit` — stop early

A short-circuit task returns **True or False**:

- **True** → keep going, run everything downstream.
- **False** → **skip everything downstream**.

```python
@task.short_circuit
def has_new_data() -> bool:
    new_rows = 0
    print(f"new rows today = {new_rows}")
    return new_rows > 0        # 0 rows -> False -> skip the rest of the pipeline

has_new_data() >> load_task()
```

Use it as a **guard**: "only run the expensive work if there's actually something
to do." This is the single biggest cost saver — don't scan and load when today's
source is empty.

Branch vs short-circuit: **branch chooses between paths; short-circuit decides
whether to continue at all.**

---

## 3. `TriggerRule` — when is a task allowed to run?

By default a task runs only when **all** its upstream tasks **succeeded**. That
rule is called `all_success`. You can change it per task:

```python
from airflow.sdk import TriggerRule

@task(trigger_rule=TriggerRule.ALL_DONE)
def cleanup() -> None:
    ...
```

The ones you'll actually use:

| Trigger rule | Task runs when… | Use it for |
|---|---|---|
| `ALL_SUCCESS` (default) | every upstream succeeded | normal flow |
| `NONE_FAILED_MIN_ONE_SUCCESS` | no upstream failed **and** ≥1 succeeded (skips OK) | a **join after a branch** |
| `ALL_DONE` | every upstream finished (success, fail, or skip) | **cleanup / notify** that must always run |
| `ONE_SUCCESS` | any one upstream succeeded | fan-in where any success is enough |
| `ALL_FAILED` | every upstream failed | run only on total failure |

---

## 4. The classic gotcha: skips flow downstream

When a branch **skips** a task, that "skipped" status **passes down** to the tasks
after it. So if you have two branches that join into one task, the join has a
skipped parent — and with the default `all_success`, the join gets skipped too,
even though the other branch succeeded.

```
choose_path ──▶ refresh_full ─────┐
           └──▶ load_incremental ─┴──▶ publish   # one parent is always skipped
```

Fix: give the join `trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS`. It means
"run as long as nothing failed and at least one parent actually ran" — exactly what
you want after a branch.

**Whenever a task sits below a branch, set its trigger rule deliberately.** This is
the #1 branching bug.

---

## 5. A real BigQuery scenario

**The situation:** a daily load job for a sales table.

1. **Guard (short-circuit):** first count today's new rows in the source. If it's
   **0**, short-circuit → skip the whole load. No point scanning and writing when
   nothing arrived (and it saves cost).
2. **Branch:** if there *is* data, decide *how* to load based on volume — a small
   batch takes the `refresh_full` path, a large one takes `load_incremental`.
3. **Join (publish):** after whichever path ran, one task publishes/marks the load
   done — with `NONE_FAILED_MIN_ONE_SUCCESS`, so the skipped branch doesn't skip it.
4. **Notify (all_done):** a final task logs the outcome and (later) sends a Slack
   message — with `ALL_DONE`, so it runs whether the load succeeded, failed, or was
   short-circuited.

```
count_new_rows ─(short-circuit: 0 rows? stop)─▶ choose_load ─┬─▶ refresh_full ──┐
                                                              └─▶ load_incr ─────┴─▶ publish ─▶ notify
                                                                        (NONE_FAILED_MIN_ONE_SUCCESS)   (ALL_DONE)
```

Map back to the road trip: the guard is the BRIDGE-OUT barrier, `choose_load` is
the fork, `publish` is a junction that proceeds if either road got through, and
`notify` is the crew that always shows up at the end.

---

## 6. Complete runnable reference DAG

Self-contained (no providers) so it runs anywhere. It shows all four pieces:
short-circuit guard → branch → join with the right trigger rule → always-run notify.

```python
from __future__ import annotations

import pendulum
from airflow.sdk import dag, task, TriggerRule


@dag(
    dag_id="s04_branching_demo",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["session-04", "branching"],
    default_args={"owner": "akhand", "retries": 1},
)
def pipeline():

    @task.short_circuit
    def has_new_data() -> bool:
        new_rows = 5000                  # pretend we counted today's source rows
        print(f"new rows today = {new_rows}")
        return new_rows > 0              # False -> skip everything below

    @task.branch
    def choose_load() -> str:
        new_rows = 5000
        # return the TASK_ID string of the path to run; the other is skipped
        return "refresh_full" if new_rows > 1000 else "load_incremental"

    @task(task_id="refresh_full")
    def full_refresh() -> None:
        print("path: full refresh")

    @task(task_id="load_incremental")
    def incremental_load() -> None:
        print("path: incremental load")

    @task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
    def publish() -> None:               # join after the branch
        print("publishing load results")

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def notify() -> None:                # always runs, even on skip/fail
        print("pipeline finished — sending status")

    gate = has_new_data()
    branch = choose_load()

    gate >> branch >> [full_refresh(), incremental_load()] >> publish() >> notify()


pipeline()
```

Run it:

```bash
python dags/task-4/s04_branching_demo.py
airflow dags test s04_branching_demo 2026-01-01
```

In the UI you'll see one branch **skipped** (grey), the other green, `publish` still
running because of its trigger rule, and `notify` running at the end. Change
`new_rows` to `0` and re-run: everything after the guard turns skipped.

---

## 7. Build spec — your challenge (no solution)

**File:** `dags/task-4/04_branching.py`  ·  **dag_id:** `s04_branching`

Build a DAG that makes a run-time decision, protects an expensive step with a
guard, and always finishes with a status task.

**The problem:**

- A **guard** task runs first and decides whether the pipeline should continue at
  all. If its condition is not met, everything after it must be **skipped**.
- If it continues, a **branch** task chooses **one of two** downstream paths based
  on a condition; the path not chosen must be **skipped**.
- Both paths lead into a single **join** task that must still run even though one
  branch was skipped.
- A final **status** task must run **no matter what** happened above (success,
  failure, or skip).

**Constraints:**

- Use `@task.short_circuit` for the guard and `@task.branch` for the path choice.
- The join and the status task must set the correct `TriggerRule` (think about what
  a skipped branch does to a default-rule join).
- Keep every Python function/variable name **different** from its `task_id` string,
  except where a branch must return a `task_id` (there the returned string names the
  target task on purpose — comment it).
- Pass the integrity gates: `tags`, a real `owner`, `retries >= 1`.

**Acceptance criteria:**

- `python dags/task-4/04_branching.py` parses (prints nothing).
- `airflow dags test s04_branching 2026-01-01` runs green.
- In the graph: exactly one branch runs, the other is skipped, the join still runs,
  and the status task runs.
- Flip the guard's condition and confirm the whole pipeline below it is skipped —
  but the always-run status task still runs.
- `python -m pytest tests/ -v` stays green.

**Nudge (only if stuck):** the shape is the §6 reference —
`guard(short_circuit) → branch → [pathA, pathB] → join(NONE_FAILED_MIN_ONE_SUCCESS) → status(ALL_DONE)`.
Change the conditions and what each path does.

---

## 8. Production tip — guards save money, trigger rules save you at 2 a.m.

- **Put a short-circuit guard in front of every expensive stage.** "Is there new
  data? Does the partition exist? Is this the right day?" A cheap check that skips a
  costly BigQuery load is the highest-leverage habit in a warehouse pipeline — you
  stop paying for work that has nothing to do.
- **Never leave a post-branch task on the default trigger rule by accident.** A join
  that silently skips because one branch was skipped is a classic production
  incident — the pipeline "succeeds" but the important step never ran. Set
  `NONE_FAILED_MIN_ONE_SUCCESS` on joins and `ALL_DONE` on cleanup/alerting on
  purpose, and write a comment saying why.

---

## 9. Verify + commit

```bash
python dags/task-4/04_branching.py
airflow dags test s04_branching 2026-01-01
python -m pytest tests/ -v
git add -A && git commit -m "session 04: branching and trigger rules" && git push
```

Done when the graph shows one path taken, one skipped, the join still running, and
the status task always running. Tick **04** in `README.md`.

Sources:
[Branching — Astronomer](https://www.astronomer.io/docs/learn/airflow-branch-operator),
[airflow.sdk API reference](https://airflow.apache.org/docs/task-sdk/stable/api.html)
