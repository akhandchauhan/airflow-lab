# Session 04 · W2 Sun — Dynamic Task Mapping

**Goal:** build a task whose *number of copies* is decided while the DAG runs, not
when you write it. This is Airflow's built-in map/reduce. You will learn
`.expand`, `.partial`, `.expand_kwargs`, `.map`, `.zip`, how each copy is numbered
by `map_index`, and how one later task collects all their results.

---

## 0. The problem it solves

Sometimes you do not know how many tasks you need until the run starts. Examples:
one task per file that landed today, one per date, one per region an API returns.
The count is different every run, so you cannot write these as a fixed set of
tasks.

You might try to loop over the list and create tasks:

```python
for f in list_files_now():      # WRONG
    process(f)
```

This is wrong. Remember from Session 01 that any code at the top level of the file
runs at **parse time**. Airflow parses the file over and over (roughly every 30
seconds, on the DAG Processor), long before and separately from any actual run. So
this loop would:

1. read the file list at the wrong time and on the wrong machine, and
2. change the shape of the DAG on its own every time the list changes.

**Dynamic task mapping** is the right tool. You write **one** task and mark it as
mappable. When a run starts and the real list is known, Airflow turns that one
task into N copies.

Think of it as **map/reduce**:

```
        ┌─ process[0]  (file a)
list ──▶├─ process[1]  (file b)   ──▶  reduce (one task, gets [r0, r1, r2])
        └─ process[2]  (file c)
   map (fan-out: N copies made at run time)     reduce (fan-in)
```

Every copy is a normal task instance. It can succeed, fail, and retry on its own.
In the UI the copies appear as `process[0]`, `process[1]`, `process[2]`. That
number in brackets is the **`map_index`**, starting at 0.

---

## 1. `.expand()` — make one copy per item

`.expand()` is the map step. You call it on a task and give it the argument you
want to change from copy to copy. Airflow makes **one copy of the task per item**
in the list.

```python
@task
def process(name: str) -> str:
    return name.upper()

process.expand(name=["a", "b", "c"])   # -> process[0]=A, process[1]=B, process[2]=C
```

- The keyword you pass (`name=`) must be the **name of a parameter** of the task
  function.
- Each item in the list becomes that parameter's value for one copy.
- `.expand()` accepts a **list**, a **dict**, or the output of another task (an
  **XComArg**). It does **not** accept a plain constant. Constant values go in
  `.partial()` (§2).

The important case is expanding over **another task's output**, which is only
known at run time:

```python
files = list_files()                 # returns e.g. ["a.csv", "b.csv"] during the run
process.expand(name=files)           # number of copies = length of that list
```

---

## 2. `.partial()` — set the values that stay the same

A mapped task usually has some arguments that **change per copy** and some that are
**the same for every copy**. The unchanging ones go in `.partial()`:

```python
@task
def load(filename: str, bucket: str, dry_run: bool) -> int:
    ...

load.partial(bucket="raw-zone", dry_run=False).expand(filename=files)
```

Every copy gets the same `bucket` and `dry_run`. Only `filename` changes.

Simple rule: **`.partial()` = same for all copies. `.expand()` = one value per
copy.** Any parameter that is not in `.expand()` must be given by `.partial()` (or
have a default). If you forget one, parsing fails.

---

## 3. Expanding over two arguments = every combination

If you expand over **two** arguments, Airflow makes a copy for **every
combination** of the two lists, not pairs:

```python
add.expand(x=[1, 2], y=[10, 20])
# -> 4 copies: (1,10) (1,20) (2,10) (2,20)
```

This grows fast: two lists of 100 items make 10,000 copies. If you instead want
**pairs** (first with first, second with second), use `.zip()` (§5) or
`.expand_kwargs()` (§4), not two `.expand()` arguments.

---

## 4. `.expand_kwargs()` — give each copy a full set of arguments

Sometimes each copy needs its own combination of several arguments, and you do not
want every combination. For that, pass a **list of dicts**. Each dict is the
complete set of keyword arguments for one copy:

```python
run_job.expand_kwargs([
    {"query": "SELECT 1", "location": "US"},
    {"query": "SELECT 2", "location": "EU"},
])
# -> 2 copies, each receiving one whole dict as its arguments
```

Use it when a copy needs specific values for several parameters together, or when
a task already returns a list of ready-made argument dicts. Difference from
`.expand()`: `.expand()` changes **one** argument at a time and makes every
combination; `.expand_kwargs()` sets the **whole** argument set for each copy.

---

## 5. `.zip()` — pair up several lists

To combine several lists into pairs (like Python's built-in `zip`), call `.zip()`
on a task's output. It produces a list of tuples that you can then expand over:

```python
names = get_names()      # ["a", "b"]
sizes = get_sizes()      # [10, 20]
paired = names.zip(sizes)          # -> [("a", 10), ("b", 20)]

@task
def handle(pair: tuple) -> None:
    name, size = pair
    ...

handle.expand(pair=paired)         # 2 copies (pairs), not 4 (combinations)
```

- It stops at the **shortest** list, just like `zip`. Pass `fillvalue=` to change
  that.
- This gives you pairs instead of the "every combination" behaviour from §3.

## 6. `.map()` — change each item before expanding

`.map()` runs a function on **each item** of a task's output and gives back a new
list, without adding a separate task. Use it to reshape one task's output into the
exact form the next task needs:

```python
paths = list_files()                         # ["a.csv", "b.csv"]
full = paths.map(lambda p: f"gs://raw/{p}")  # ["gs://raw/a.csv", "gs://raw/b.csv"]
process.expand(name=full)
```

It is like a list comprehension, but on a value that only exists during the run.
It does not create a new box in the graph.

---

## 7. Reduce — one task that collects all the copies' results

The fan-in is automatic. If a **later task that is not mapped** takes a mapped
task's output as input, it receives the **full list** of results from all the
copies:

```python
counts = process.expand(name=files)   # mapped; each copy returns an int

@task
def total(values: list[int]) -> int:  # not mapped -> receives [r0, r1, r2, ...]
    return sum(values)

total(counts)
```

`total` runs **once**, after every `process[i]` has finished, and gets a list of
their return values. This is the reduce step. There is no special function for it.
You just feed a mapped task's output into a normal task.

---

## 8. Guardrails — this creates real work

Every copy is a real task the scheduler must track. If the list is huge or has no
limit, you can overload the scheduler and the database. Controls:

- **`max_map_length`** (a core setting, default **1024**) is the hard limit on how
  many copies one `.expand()` may create. If a run tries to make more, it **fails
  with a clear error** instead of quietly overloading the system. Raise it only on
  purpose, never by accident.
- **`max_active_tis_per_dag`** (set on the task) limits how many copies run **at
  the same time**. It throttles concurrency without changing the total count.
- **`max_active_tis_per_dagrun`** does the same, but counted per DAG run.

If the list comes from outside (a query, an API), **put a limit on it** (a `LIMIT`
or a slice) before it reaches `.expand()`.

---

## 9. Complete runnable reference DAG

This is self-contained (no providers) so it runs anywhere. It shows `.expand` +
`.partial` + a reduce.

Notice the **naming**: the Python function is `count_rows`, but its `task_id` is a
**different** string, `"row_count"`. Keeping them different makes it clear that
`.partial` and `.expand` are called on the **function**, while `"row_count"` is
only the id.

```python
from __future__ import annotations

import pendulum
from airflow.sdk import dag, task


@dag(
    dag_id="s04_dynamic_mapping_demo",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["session-04", "dynamic-mapping"],
    default_args={"owner": "akhand", "retries": 1},
)
def pipeline():

    @task
    def list_files() -> list[str]:
        # in real life this reads a bucket/dir AT RUN TIME
        return ["orders.csv", "users.csv", "products.csv"]

    @task(task_id="row_count")            # task_id != function name (count_rows)
    def count_rows(filename: str, source_zone: str) -> int:
        # source_zone is the same for all copies (partial); filename changes (expand)
        fake_sizes = {"orders.csv": 1200, "users.csv": 300, "products.csv": 80}
        print(f"counting {source_zone}/{filename}")
        return fake_sizes[filename]

    @task
    def total_rows(counts: list[int]) -> int:      # reduce: receives the whole list
        total = sum(counts)
        print(f"total rows across {len(counts)} files = {total:,}")
        return total

    files = list_files()
    counts = count_rows.partial(source_zone="raw").expand(filename=files)
    total_rows(counts)


pipeline()
```

Run it:

```bash
python dags/task-4/s04_dynamic_mapping_demo.py
airflow dags test s04_dynamic_mapping_demo 2026-01-01
```

In the UI, `row_count` shows as `row_count[0]`, `row_count[1]`, `row_count[2]`, and
`total_rows` runs once with the list `[1200, 300, 80]`.

---

## 10. Build spec — your challenge (no solution)

**File:** `dags/task-4/04_dynamic_mapping.py`  ·  **dag_id:** `s04_dynamic_mapping`

Build a DAG that spreads work across a list whose length is only known during the
run, then collects the results. The number of parallel copies must come from the
run, not be hard-coded.

**The problem:**

- A first task returns a **list** during the run. Its length is not known when the
  DAG is written (for example a list of shards, filenames, or region codes).
- A second task is **mapped** over that list, so there is one copy per item. Each
  copy does some work and returns a value. This task must also take at least one
  argument that is the **same for every copy**.
- A third task is **not mapped**. It takes all the results from the second task,
  combines them into one value, and logs it.

**Constraints:**

- The number of copies must come from the first task's output. Do not pass a
  hard-coded list to `.expand()`, and do not create tasks with a Python loop at the
  top level.
- Put the constant arguments in `.partial()` and the changing argument in
  `.expand()`.
- Keep every Python function/variable name **different** from its `task_id` string.
- Pass the integrity gates: `tags`, a real `owner`, `retries >= 1`.

**Acceptance criteria:**

- `python dags/task-4/04_dynamic_mapping.py` parses (prints nothing).
- `airflow dags test s04_dynamic_mapping 2026-01-01` runs green.
- The UI shows the mapped task as numbered copies `name[0]`, `name[1]`, … and the
  number of copies equals the length of the first task's list.
- The third task runs **once** and receives the list of all the copies' results.
- `python -m pytest tests/ -v` stays green.

**Stretch (optional):** make the first task return a list of **dicts** and use
`.expand_kwargs()` instead of `.partial().expand()`. Or combine two lists into
pairs with `.zip()` and check that you get N copies, not N×M.

**Nudge (only if stuck):** the shape is the same as the §9 reference —
`producer → worker.partial(const=...).expand(var=producer()) → reducer`. Change
*what* the producer returns and *what* the worker computes.

---

## 11. Production tip — limit the fan-out, and make each copy safe to re-run

Dynamic mapping is a common way for a DAG to accidentally overload a cluster. Two
habits keep it safe:

- **Always cap the source list.** The number of copies should have a known upper
  bound: a `LIMIT` on the query, a slice, or a validated input. Treat
  `max_map_length` (1024) as a safety net, not your plan. If a run really needs
  5,000 copies, redesign it to work in batches instead of raising the limit. Use
  `max_active_tis_per_dag` so that, say, 500 copies do not all hit the same
  downstream API at once.
- **Each copy must be independent and safe to re-run.** Copy `[k]` should use only
  its own item, never the results of other copies or shared changing state.
  Re-running it must be safe, so write its output to a place tied to that item (a
  per-item or per-partition destination). That is what lets a single failed
  `name[37]` retry cleanly without breaking the other copies.

---

## 12. Verify + commit

```bash
python dags/task-4/04_dynamic_mapping.py
airflow dags test s04_dynamic_mapping 2026-01-01
python -m pytest tests/ -v
git add -A && git commit -m "session 04: dynamic task mapping" && git push
```

Done when the DAG runs green, the UI shows the run-time number of copies, and the
third task collects them all. Tick **04** in `README.md`.

Sources:
[Dynamic Task Mapping — Airflow 3.3 docs](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/dynamic-task-mapping.html),
[Task SDK — Dynamic Task Mapping](https://airflow.apache.org/docs/task-sdk/stable/dynamic-task-mapping.html)
