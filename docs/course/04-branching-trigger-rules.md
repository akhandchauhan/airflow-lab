# 04 · Branching & Trigger Rules

**One line:** this is how a DAG makes **decisions** — take one path or another,
stop early when a check fails, and control **when** a task is allowed to run based
on what happened before it.

Three tools:

| Tool                  | Plain meaning                                                                       |
| --------------------- | ----------------------------------------------------------------------------------- |
| `@task.branch`        | a **fork in the road** — pick which path to take; the other path is skipped         |
| `@task.short_circuit` | a **stop sign** — if a check is False, skip everything after it                     |
| `TriggerRule`         | the rule for **when** a task may start (default: only after all its inputs succeed) |

---

## 0. The analogy: a road trip

- **Branch** = a fork in the road. A sign sends you either left or right. The road
  you don't take is closed off (its tasks are **skipped**).
- **Short-circuit** = a "BRIDGE OUT" barrier. If the bridge is out (your check
  returns False), you stop, and everything further down that road is cancelled.
- **Trigger rule** = the rule at a junction for _when you're allowed to go_.
  Normally: "go only when all the roads feeding in are clear." But you can change
  it — for example a cleanup crew that goes in _no matter what happened_.

Keep this picture; every section below maps back to it.

> The snippets in §1–§4 **highlight one mechanic each** — just the lines that
> matter, not a full DAG. The single complete, runnable DAG that wires everything
> together is the BigQuery reference in **§6**. After each mechanic, a **🎯 Challenge**
> asks you to reuse something from an earlier session.

---

## 1. `@task.branch` — pick a path

A branch task is a normal `@task`, but instead of returning data it **returns the
`task_id`** (a string) of the task you want to run next. Every other task directly
below the branch is marked **skipped**.

```python
@task.branch                                   # ← THE MECHANIC
def check_count() -> str:
    rows = 5000
    return "incremental_load" if rows > 500 else "full_reload"   # returns a TASK_ID string

@task(task_id="incremental_load")
def run_incremental_load():
    print("incremental load")

@task(task_id="full_reload")
def run_full_reload():
    print("truncate + full reload")

path = check_count()
path >> [run_incremental_load(), run_full_reload()]   # the returned task_id runs; the other is skipped
```

- The branch returns a **`task_id` string**, not the function object.
- Return a **list** of task_ids to run several paths at once.
- The branch and its options must be **directly wired** (`path >> [a, b]`), or
  Airflow can't skip the right ones.

> **🎯 Challenge — reuse Session 01 (XCom hand-off).** The branch above reads a
> hardcoded `rows`. Replace it: add an upstream `@task` that **returns** the row
> count, and make `check_count(count)` receive that value as an argument — the
> return→XCom hand-off from Session 01. The branch must now decide from the real
> returned value, not a constant. (Refresh: [xcom-basics](xcom-basics.md).)

---

## 2. `@task.short_circuit` — stop early

A short-circuit task returns **True or False**:

- **True** → keep going, run everything downstream.
- **False** → **skip everything downstream**.

```python
@task.short_circuit                # ← THE MECHANIC
def has_new_data() -> bool:
    new_rows = 0
    return new_rows > 0            # return False → EVERYTHING downstream is skipped

@task
def load_data():
    print("loading data")

has_new_data() >> load_data()
```

Use it as a **guard**: "only run the expensive work if there's actually something
to do." This is the single biggest cost saver — don't scan and load when today's
source is empty.

Branch vs short-circuit: **branch chooses between paths; short-circuit decides
whether to continue at all.**

> **🎯 Challenge — reuse Session 03 (TaskGroups).** Put the downstream work inside a
> `@task_group` (an `extract` task + a `load` task grouped together) and wire the
> short-circuit guard **before** the group. Confirm that when the guard returns
> False, the **whole group** is skipped in one shot — not task by task.

---

## 3. `TriggerRule` — when is a task allowed to run?

By default a task runs only when **all** its upstream tasks **succeeded** — that
rule is `all_success`. You change it per task with `trigger_rule`:

```python
from airflow.sdk import TriggerRule

@task
def load_data():
    print("loading data")

@task(trigger_rule=TriggerRule.ALL_DONE)     # ← THE MECHANIC: change WHEN a task may run
def cleanup():
    print("cleanup runs even if load_data FAILED or was skipped")

load_data() >> cleanup()
```

With `ALL_DONE`, `cleanup` runs no matter what happens to `load_data`. On the
default `ALL_SUCCESS`, a failed or skipped `load_data` would skip `cleanup` too.

The trigger rules you'll actually use:

| Trigger rule                  | Task runs when…                                    | Use it for                                |
| ----------------------------- | -------------------------------------------------- | ----------------------------------------- |
| `ALL_SUCCESS` (default)       | every upstream succeeded                           | normal flow                               |
| `NONE_FAILED_MIN_ONE_SUCCESS` | no upstream failed **and** ≥1 succeeded (skips OK) | a **join after a branch**                 |
| `ALL_DONE`                    | every upstream finished (success, fail, or skip)   | **cleanup / notify** that must always run |
| `ONE_SUCCESS`                 | any one upstream succeeded                         | fan-in where any success is enough        |
| `ALL_FAILED`                  | every upstream failed                              | run only on total failure                 |

> **🎯 Challenge — reuse Session 02 (parallel wiring).** Using the parallel style
> from Session 02 (`[task_a, task_b] >> join`), build two parallel tasks where
> **one raises an error**, both feeding a `summary` task. Leave `summary` on the
> default rule and watch it get skipped; then pick the trigger rule that makes
> `summary` run because at least one parent succeeded. Which fits — `ONE_SUCCESS`
> or `ALL_DONE`? Explain the difference between them.

---

## 4. The classic gotcha: skips flow downstream

When a branch **skips** a task, that "skipped" status **passes down** to the tasks
after it. So if you have two branches that join into one task, the join has a
skipped parent — and with the default `all_success`, the join gets skipped too,
even though the other branch succeeded.

```
pick_load_path ──▶ full_refresh ─────┐
              └──▶ incremental_load ─┴──▶ publish   # one parent is always skipped
```

Fix: give the join `trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS`. It means
"run as long as nothing failed and at least one parent actually ran" — exactly what
you want after a branch.

**Whenever a task sits below a branch, set its trigger rule deliberately.** This is
the #1 branching bug.

> **🎯 Challenge — reuse P1 (BigQuery).** Feed the §1 branch a **real** number: use
> `BigQueryHook.get_first` (from P1) to count rows in
> `bigquery-public-data.austin_bikeshare.bikeshare_trips`, and branch on that live
> count instead of a constant. Add a join after the two paths and give it the right
> trigger rule so the skipped branch doesn't skip it. This is your bridge into §6.

---

## 5. A real BigQuery scenario

**The situation:** a daily load job for a sales table.

1. **Guard (short-circuit):** first count today's new rows in the source. If it's
   **0**, short-circuit → skip the whole load. No point scanning and writing when
   nothing arrived (and it saves cost).
2. **Branch:** if there _is_ data, decide _how_ to load based on volume — a small
   batch takes the `full_refresh` path, a large one takes `incremental_load`.
3. **Join (publish):** after whichever path ran, one task publishes/marks the load
   done — with `NONE_FAILED_MIN_ONE_SUCCESS`, so the skipped branch doesn't skip it.
4. **Notify (all_done):** a final task logs the outcome and (later) sends a Slack
   message — with `ALL_DONE`, so it runs whether the load succeeded, failed, or was
   short-circuited.

```
count_new_rows ─(short-circuit: 0 rows? stop)─▶ choose_load ─┬─▶ full_refresh ────┐
                                                              └─▶ incremental_load ┴─▶ publish ─▶ notify
                                                                        (NONE_FAILED_MIN_ONE_SUCCESS)   (ALL_DONE)
```

Map back to the road trip: the guard is the BRIDGE-OUT barrier, `choose_load` is
the fork, `publish` is a junction that proceeds if either road got through, and
`notify` is the crew that always shows up at the end.

---

## 6. Complete runnable reference DAG (BigQuery)

Uses the `google_cloud_default` connection from **P1** and the Austin bikeshare
table. It shows all four pieces on real data: short-circuit guard → branch → join
with the right trigger rule → always-run notify. Every query is cost-capped.

```python
from __future__ import annotations

import pendulum
from airflow.sdk import dag, task, TriggerRule
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

SRC = "bigquery-public-data.austin_bikeshare.bikeshare_trips"
CAP = "100000000"          # 100 MB max bytes billed per query
BIG_THRESHOLD = 1_000_000  # above this many trips, take the "large" path


@dag(
    dag_id="s04_branching_demo",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["session-04", "branching", "bigquery"],
    default_args={"owner": "akhand", "retries": 1},
)
def pipeline():

    @task
    def count_trips() -> int:
        hook = BigQueryHook(gcp_conn_id="google_cloud_default", location="US", use_legacy_sql=False)
        row = hook.get_first(f"SELECT COUNT(*) FROM `{SRC}`")   # COUNT = 0 bytes
        return int(row[0])

    @task.short_circuit
    def has_rows(total: int) -> bool:
        print(f"total trips = {total:,}")
        return total > 0                # 0 rows -> skip everything below

    @task.branch
    def choose_by_volume(total: int) -> str:
        # return the TASK_ID string of the path to run; the other is skipped
        return "summarize_large" if total > BIG_THRESHOLD else "summarize_small"

    # variable name (large_path) is kept different from the task_id ("summarize_large"),
    # which is the string the branch returns
    large_path = BigQueryInsertJobOperator(
        task_id="summarize_large",
        gcp_conn_id="google_cloud_default",
        location="US",
        configuration={"query": {
            "query": f"SELECT start_station_name, COUNT(*) AS trips FROM `{SRC}` "
                     f"GROUP BY start_station_name ORDER BY trips DESC LIMIT 10",
            "useLegacySql": False, "maximumBytesBilled": CAP,
        }},
    )

    small_path = BigQueryInsertJobOperator(
        task_id="summarize_small",
        gcp_conn_id="google_cloud_default",
        location="US",
        configuration={"query": {
            "query": f"SELECT start_station_name, COUNT(*) AS trips FROM `{SRC}` "
                     f"GROUP BY start_station_name ORDER BY trips DESC LIMIT 3",
            "useLegacySql": False, "maximumBytesBilled": CAP,
        }},
    )

    @task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
    def publish() -> None:               # join after the branch
        print("published summary")

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def notify() -> None:                # always runs, even on skip/fail
        print("pipeline finished — sending status")

    total = count_trips()
    guard = has_rows(total)
    decision = choose_by_volume(total)

    guard >> decision >> [large_path, small_path] >> publish() >> notify()


pipeline()
```

Run it (needs the P1 BigQuery connection):

```bash
python dags/task-4/s04_branching_demo.py
airflow dags test s04_branching_demo 2026-01-01
```

Austin bikeshare has ~2.3M trips (> `BIG_THRESHOLD`), so `summarize_large` runs and
`summarize_small` is **skipped** (grey in the UI); `publish` still runs because of
its trigger rule; `notify` runs at the end. Lower `BIG_THRESHOLD` to flip the
branch, and check **BigQuery Job history** — `count_trips` = 0 B, the summary query
scans only the station column, under the cap.

---

## 7. Build spec — your challenge (BigQuery, no solution)

**File:** `dags/task-4/04_branching.py` · **dag_id:** `s04_branching`

Build a DAG that queries BigQuery, makes a run-time decision from the result,
protects an expensive step with a guard, and always finishes with a status task.
Use `bigquery-public-data.austin_bikeshare.bikeshare_trips` (or a table in your own
dataset).

**The problem:**

- A first task reads a **real metric from BigQuery** (for example a row count, or a
  count of trips for some condition).
- A **guard** decides from that metric whether the pipeline should continue. If the
  condition is not met (e.g. the metric is 0), everything after it must be
  **skipped**.
- If it continues, a **branch** chooses **one of two** BigQuery query paths based on
  the metric (e.g. a heavier aggregation vs a lighter one); the path not chosen must
  be **skipped**.
- Both paths lead into a single **join** task that must still run even though one
  branch was skipped.
- A final **status** task must run **no matter what** happened above (success,
  failure, or skip).

**Constraints:**

- The BigQuery work uses the Google provider — `BigQueryInsertJobOperator` for the
  query paths and/or `BigQueryHook` for reading the metric — via
  `google_cloud_default`.
- **Every query is cost-capped** (`maximumBytesBilled`); no `SELECT *`.
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
- **BigQuery Job history** shows your queries ran within the byte cap.
- Flipping the guard's condition (or the branch threshold) changes the outcome as
  expected.
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
