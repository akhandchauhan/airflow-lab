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

### (a) `@task_group` decorator - pairs with TaskFlow (what we use)

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
joins the group. (Shown for awareness - this session builds in TaskFlow.)

```python
with TaskGroup(group_id="ingest") as ingest:
    download = EmptyOperator(task_id="download")
    stage    = EmptyOperator(task_id="stage")
    download >> stage
```

Both produce the same collapsible "ingest" node.

---

## 3. `group_id` prefixing - the part that trips people up

Inside a group, a task's **real id becomes `group_id.task_id`.** So `download`
inside group `ingest` is actually `ingest.download`. This is how Airflow keeps
ids unique even if two groups both have a `validate` task (`orders.validate` vs
`users.validate`).

Consequences:

- In `airflow tasks test`, you reference the **full** id: `airflow tasks test <dag> ingest.download 2026-01-01`.
- The prefix is why you can **reuse the same group definition** for multiple inputs without id collisions.
- `prefix_group_id=False` on the group turns prefixing off (rarely needed; you then must keep ids unique yourself).

### Function name vs `group_id`

Like `@dag`/`dag_id`, they are independent:

- The **function name** is a Python label, cosmetic.
- **`group_id`** is the group's real identity (the prefix on child task ids).

They can differ. And if you omit `group_id`, the **function name becomes it**:

```python
@task_group(group_id="ingest")
def whatever():        # function name irrelevant -> prefix is "ingest"
    ...

@task_group            # no group_id
def orders():          # function name IS the group_id -> prefix is "orders"
    ...
```

Naming them the same (`orders` / `"orders"`) is just readability convention, not
a requirement.

---

## 4. Nesting groups (TaskFlow)

Groups nest. A `@task_group` defined inside another `@task_group` prefixes twice:

```python
@task_group(group_id="orders")
def orders():
    @task
    def download() -> str:                     # -> orders.download
        return "/raw/orders"

    @task_group(group_id="quality")
    def quality(path: str):
        @task
        def check_nulls(p: str) -> str:        # -> orders.quality.check_nulls
            return p

        @task
        def check_schema(p: str) -> str:       # -> orders.quality.check_schema
            return p

        check_nulls(path)
        check_schema(path)

    quality(download())                        # download's output flows into the nested group

orders()
```

The id builds up one prefix per nesting level: `orders.quality.check_nulls`. The
UI shows `orders` collapsing to reveal `download` and a nested `quality` box.

Note the wiring is pure TaskFlow: `quality(download())` passes `download`'s
XComArg into the nested group, which draws the edge automatically (Session 01) -
no `>>` needed.

---

## 5. Wiring groups together

You can set dependencies on **whole groups** just like tasks - `>>` works on a
group object and wires its boundary tasks:

```python
start >> group_a >> group_b >> end
```

In pure TaskFlow you usually wire by **data flow** instead - pass one group's
output into the next (`stage(validate(download(name)))`), same as tasks. Use `>>`
on groups only when there's no data to thread.

### Instantiating the same group many times

Calling a `@task_group` function twice would collide on `group_id`. Give each a
distinct id with `.override(group_id=...)`:

```python
for name in ["orders", "users", "products"]:
    ingest.override(group_id=f"ingest_{name}")(name)
```

---

## 6. A complete runnable DAG (your reference, TaskFlow)

A whole file, end to end, before the spec. One parametrized `@task_group`,
instantiated twice with `.override(group_id=...)`.

```python
from __future__ import annotations

import pendulum
from airflow.sdk import dag, task, task_group


@dag(
    dag_id="task_group_demo",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["course", "demo"],
    default_args={"owner": "akhand", "retries": 2},
)
def pipeline():

    @task_group(group_id="ingest")
    def ingest(name: str) -> None:
        @task
        def download(src: str) -> str:         # -> ingest_orders.download
            return f"/raw/{src}"

        @task
        def validate(path: str) -> str:        # -> ingest_orders.validate
            return path

        @task
        def stage(path: str) -> None:          # -> ingest_orders.stage
            print(f"staging {path}")

        stage(validate(download(name)))        # TaskFlow wiring inside the group

    # instantiate the same group per source, distinct group_id each time
    for name in ["orders", "users"]:
        ingest.override(group_id=f"ingest_{name}")(name)


pipeline()
```

Read the shape:

- `@task_group(group_id="ingest")` defines the group once; `.override(group_id=...)`
  gives each call a unique id so the two instances don't collide.
- Inside, wiring is pure TaskFlow - `stage(validate(download(name)))` draws
  `download -> validate -> stage` via data flow (Session 01), no `>>`.
- Task ids come out prefixed: `ingest_orders.download`, `ingest_users.stage`, ...
- No SubDAG, no extra scheduling cost - just structure.

Run it:

```bash
python dags/task_group_demo.py
airflow dags test task_group_demo 2026-01-01
```

---

## 7. Build spec - you write this (no solution, TaskFlow)

Create `dags/03_task_groups.py`, dag_id `03_task_groups`. Build a multi-source
ingestion DAG in **pure TaskFlow** that uses **a parametrized group AND a nested
group**.

**Structure:**
- A `@task_group` `ingest(name)`, instantiated for **three** sources: `orders`,
  `users`, `products` - each with a distinct `group_id` (e.g. `ingest_orders`).
- Inside each `ingest` group:
  - `@task download(src)` -> returns a path string.
  - a **nested** `@task_group quality(path)` containing two `@task`s,
    `check_nulls(path)` and `check_schema(path)`, each returning `path`.
  - `@task stage(path)` -> prints "staged {path}".
  - wired by data flow: `download`'s output feeds `quality`, whose output feeds
    `stage`.

**The thinking part:**
- Instantiate the same group three times without `group_id` collisions using
  `.override(group_id=f"ingest_{name}")(name)`.
- Nest `quality` inside `ingest` and confirm the ids come out as
  `ingest_orders.quality.check_nulls`, etc.
- Thread the path through the nested group with TaskFlow data flow - the nested
  group takes `download`'s output and returns something `stage` consumes. (Decide
  what the nested group should return so `stage` gets a single path - the two
  checks both return `path`, so pick one, or have `quality` return the path it
  was given.)

**Constraints:**
- Pure TaskFlow - `@task_group` + `@task`, no `EmptyOperator`, no `>>`.
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
