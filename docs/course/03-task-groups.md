# Session 03 - W2 Sat - TaskGroups

**Goal:** organize a DAG's tasks into named, collapsible groups - understand that
a TaskGroup is a *visual/logical* wrapper (not a sub-DAG), how `group_id`
prefixes task ids, how to nest groups, and how to instantiate the same group
many times.

---

## 1. What is a TaskGroup? (and what it is NOT)

A **TaskGroup is a way to bundle tasks under one collapsible node in the UI.**
It is purely **organizational** - it groups tasks visually in the Graph and
namespaces their ids. It does **not** change how tasks are scheduled or executed;
every task inside still runs as an independent task in the same DAG.

Critically, a TaskGroup is **not** a sub-DAG. (Airflow 2 had `SubDagOperator`,
which ran a whole child DAG as one task and caused deadlocks - it was **removed
in Airflow 3**. TaskGroups replaced it precisely because they add zero execution
overhead: they are just a label + a namespace.)

```
Without groups (flat)              With a TaskGroup "ingest"
-------------------------          -------------------------------
download                           ingest            <- collapsible node
validate                             ingest.download
stage                                ingest.validate
                                     ingest.stage
```

Use them when a DAG has enough tasks that the Graph view becomes noise - collapse
each logical stage into one box.

---

## 2. Two ways to make a TaskGroup

### (a) `@task_group` decorator - pairs with TaskFlow

`from airflow.sdk import task_group`. Decorate a function; every `@task` defined
and called inside becomes a member of the group.

```python
@task_group(group_id="ingest")
def ingest():
    @task
    def download() -> str:
        return "/raw/file"

    @task
    def stage(path: str) -> None:
        print(f"staging {path}")

    stage(download())        # wiring inside the group, TaskFlow-style
```

### (b) `TaskGroup` context manager - pairs with classic operators

`from airflow.sdk import TaskGroup`. Everything created inside the `with` block
joins the group.

```python
with TaskGroup(group_id="ingest") as ingest:
    download = EmptyOperator(task_id="download")
    stage    = EmptyOperator(task_id="stage")
    download >> stage
```

Both produce the same collapsible "ingest" node. Use the decorator with `@task`
Python logic; use the context manager with classic operators.

---

## 3. `group_id` prefixing - the part that trips people up

Inside a group, a task's **real id becomes `group_id.task_id`.** So `download`
inside group `ingest` is actually `ingest.download`. This is how Airflow keeps
ids unique even if two groups both have a `validate` task (`orders.validate` vs
`users.validate`).

Consequences:

- In `airflow tasks test`, you reference the **full** id: `airflow tasks test <dag> ingest.download 2026-01-01`.
- The prefix is why you can **reuse the same group definition** for multiple inputs without id collisions.
- `prefix_group_id=False` on the TaskGroup turns prefixing off (rarely needed; you then must keep ids unique yourself).

---

## 4. Nesting groups

Groups nest. A group inside a group prefixes twice:

```python
with TaskGroup(group_id="orders") as orders:
    download = EmptyOperator(task_id="download")     # -> orders.download
    with TaskGroup(group_id="quality") as quality:
        check_nulls = EmptyOperator(task_id="check_nulls")  # -> orders.quality.check_nulls
```

The UI shows `orders` collapsing to reveal `download` and a nested `quality` box.

---

## 5. Wiring groups together

You set dependencies on **whole groups** just like tasks - `>>` works on a group
object and wires its boundary tasks:

```python
start >> group_a >> group_b >> end
```

`start >> group_a` means "start before every entry task of group_a"; `group_a >>
group_b` chains the groups' edges. Inside each group you wire members
separately.

### Instantiating the same group many times

Calling a `@task_group` function twice would collide on `group_id`. Give each a
distinct id with `.override(group_id=...)`:

```python
for name in ["orders", "users", "products"]:
    ingest.override(group_id=f"ingest_{name}")(name)
```

Or, with the context manager, just use an f-string id in a loop:

```python
for name in ["orders", "users", "products"]:
    with TaskGroup(group_id=f"ingest_{name}"):
        ...
```

---

## 6. A complete runnable DAG (your reference)

A whole file, end to end, before the spec. One parametrized group, instantiated
twice, wired between a start and an end marker.

```python
from __future__ import annotations

import pendulum
from airflow.sdk import DAG, TaskGroup
from airflow.providers.standard.operators.empty import EmptyOperator

with DAG(
    dag_id="task_group_demo",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["course", "demo"],
    default_args={"owner": "akhand", "retries": 2},
) as dag:

    start = EmptyOperator(task_id="start")
    end   = EmptyOperator(task_id="end")

    groups = []
    for name in ["orders", "users"]:
        with TaskGroup(group_id=f"ingest_{name}") as tg:
            download = EmptyOperator(task_id="download")   # -> ingest_orders.download
            validate = EmptyOperator(task_id="validate")
            stage    = EmptyOperator(task_id="stage")
            download >> validate >> stage
        groups.append(tg)

    start >> groups >> end     # start before both groups; both groups before end
```

Read the shape:

- Each `with TaskGroup(...)` creates one collapsible node; tasks inside are
  prefixed `ingest_orders.*`, `ingest_users.*`.
- `groups` is a list of the two group objects; `start >> groups >> end` fans out
  to both groups and fans them back into `end` - groups behave like tasks in
  wiring.
- No SubDAG, no extra scheduling cost - just structure.

Run it:

```bash
python dags/task_group_demo.py
airflow dags test task_group_demo 2026-01-01
```

---

## 7. Build spec - you write this (no solution)

Create `dags/03_task_groups.py`, dag_id `03_task_groups`. Build a multi-source
ingestion DAG that uses **parametrized groups AND a nested group**.

**Structure:**
- A `start` `EmptyOperator` and a `merge` `EmptyOperator` at the top level.
- A group per source, for **three** sources: `orders`, `users`, `products`.
  Each source group (`group_id` like `ingest_orders`) contains:
  - `download` (EmptyOperator)
  - a **nested** group `quality` containing `check_nulls` and `check_schema`
  - `stage` (EmptyOperator)
  - wired: `download >> quality-group >> stage`
- Wire: `start >> [all three source groups] >> merge`.

**The thinking part:**
- Instantiate the same source-group structure three times without `group_id`
  collisions (loop + f-string id, or `.override`).
- Nest the `quality` group inside each source group and confirm the ids come out
  as `ingest_orders.quality.check_nulls`, etc.
- Wire a nested group between two tasks (`download >> quality >> stage`) - the
  group object goes in the middle of the chain.

**Constraints:**
- All tasks `EmptyOperator`.
- Integrity gates pass: `tags`, real `owner`, `retries >= 1` via `default_args`.

**Acceptance criteria:**
- `python dags/03_task_groups.py` parses cleanly.
- UI Graph shows three collapsible `ingest_*` groups, each with a nested
  `quality` box between `download` and `stage`.
- `airflow tasks test 03_task_groups ingest_orders.quality.check_nulls 2026-01-01`
  runs (proves the nested prefixed id is correct).
- `airflow dags test 03_task_groups 2026-01-01` runs everything green.
- `python -m pytest tests/ -v` stays green.

---

## 8. Production tip - groups organize, they don't isolate

A TaskGroup is cosmetic + namespacing only. It does **not** give you per-group
concurrency limits, retries, or resource isolation - those live on tasks
(`pool`, `priority_weight`, `retries`) or the DAG. Don't reach for TaskGroups
expecting isolation; reach for them to make a 50-task Graph readable. For real
isolation between workloads, that's pools/queues/executors (Sessions 13, 24).

Also: keep `group_id`s meaningful and stable - they become part of every child
task's id, so renaming a group renames all its tasks' history.

---

## 9. Verify + commit

```bash
python dags/03_task_groups.py
airflow dags test 03_task_groups 2026-01-01
python -m pytest tests/ -v
git add dags/03_task_groups.py && git commit -m "course: 03 task groups" && git push
```

Done when CI is green. Tick session 03 in `docs/course/README.md`.

**Pre-push habit:** run
`ruff check dags/ include/ tests/ --select E,F,AIR3 && python -m pytest tests/ -v`
before every push - green locally means green CI, no failure emails.
