# Session 04 · W2 Sun — Dynamic Task Mapping

**Goal:** create tasks whose *number* is decided at **run time**, not when the DAG
is written — Airflow's built-in map/reduce. Understand `.expand`, `.partial`,
`.expand_kwargs`, `.map`, `.zip`, how mapped instances are addressed by
`map_index`, and how a downstream task **reduces** all their outputs.

---

## 0. The problem it solves

You often don't know *how many* tasks you need until the DAG is running: "one task
per file that landed today", "one per date shard", "one per region returned by an
API". You can't write those as fixed tasks — the count changes every run.

Naively you'd loop in top-level code:

```python
for f in list_files_now():      # BAD: runs at PARSE time, every 30s, on the DAG processor
    process(f)
```

That's wrong for two reasons from Session 01: top-level code runs at **parse
time** (the file list is read on the wrong machine, at the wrong moment), and the
DAG's shape would change silently between parses. **Dynamic task mapping** is the
correct mechanism: you declare *one* mapped task at parse time, and Airflow
expands it into N instances at **run time**, once it actually knows the list.

Mental model — it's **map/reduce**:

```
        ┌─ process[0]  (file a)
list ──▶├─ process[1]  (file b)   ──▶  reduce (one task, gets [r0, r1, r2])
        └─ process[2]  (file c)
   map (fan-out, N run-time instances)        reduce (fan-in)
```

Each mapped instance is a real, independently-retryable TaskInstance, shown in the
UI as `process[0]`, `process[1]`, … indexed by **`map_index`** (0-based).

---

## 1. `.expand()` — fan out over a collection

`.expand()` is the map step. Call it on a task, passing the argument(s) to vary.
Airflow creates **one task instance per element**.

```python
@task
def process(name: str) -> str:
    return name.upper()

process.expand(name=["a", "b", "c"])   # -> process[0]=A, process[1]=B, process[2]=C
```

- The keyword you pass (`name=`) must match a **parameter** of the task function.
- Each element becomes that parameter's value for one instance.
- `.expand()` accepts a **list**, a **dict**, or — the real use — an **XComArg**
  (another task's output). It does **not** accept a plain constant; constants go
  in `.partial()` (§2).

The killer feature: the list can be **another task's return value**, resolved at
run time:

```python
files = list_files()                 # returns e.g. ["a.csv", "b.csv"] at run time
process.expand(name=files)           # N is whatever list_files() produced this run
```

---

## 2. `.partial()` — freeze the constant arguments

A mapped task usually has some args that **vary** (expanded) and some that stay
**constant** across every instance. Constants go in `.partial()`:

```python
@task
def load(filename: str, bucket: str, dry_run: bool) -> int:
    ...

load.partial(bucket="raw-zone", dry_run=False).expand(filename=files)
```

Here every instance gets the same `bucket` and `dry_run`; only `filename` varies.
Rule of thumb: **`.partial()` = same for all, `.expand()` = one per element.** Any
parameter not in `.expand()` must be supplied by `.partial()` (or have a default),
or parsing fails.

---

## 3. Expanding over multiple args = Cartesian product

If you pass **two** expanded arguments, Airflow maps over the **cross product**
(every combination), not pairwise:

```python
add.expand(x=[1, 2], y=[10, 20])
# -> 4 instances: (1,10) (1,20) (2,10) (2,20)
```

This is easy to blow up — `[100] × [100]` = 10,000 instances. If you want
**pairwise** (element 0 with element 0, etc.), that's `.zip()` (§5) or
`.expand_kwargs()` (§4), not two `.expand()` args.

---

## 4. `.expand_kwargs()` — one dict of kwargs per instance

When you want to control **all** arguments of each instance explicitly (and avoid
the cross product), pass a **list of dicts** — each dict is the full keyword-args
for one instance:

```python
run_job.expand_kwargs([
    {"query": "SELECT 1", "location": "US"},
    {"query": "SELECT 2", "location": "EU"},
])
# -> 2 instances, each getting exactly that dict as kwargs
```

Use it when instances need *different combinations* of several parameters, or when
a task returns a list of ready-made kwarg dicts. `.expand()` varies one key at a
time (cross product); `.expand_kwargs()` varies the whole kwarg set per instance.

---

## 5. `.zip()` — pair multiple sources element-wise

To combine several lists **pairwise** (like Python's `zip`) into one mappable
sequence of tuples, call `.zip()` on an XComArg:

```python
names = get_names()      # ["a", "b"]
sizes = get_sizes()      # [10, 20]
paired = names.zip(sizes)          # -> [("a", 10), ("b", 20)]  (an XComArg)

@task
def handle(pair: tuple) -> None:
    name, size = pair
    ...

handle.expand(pair=paired)         # 2 instances, not 4
```

- Stops at the **shortest** input (like `zip`), unless you pass `fillvalue=`.
- This is how you get pairwise fan-out instead of the §3 cross product.

## 6. `.map()` — transform each element lazily before expanding

`.map()` applies a function to **each element** of an XComArg, producing a new
XComArg — without a separate task. Use it to reshape a task's output into exactly
what `.expand()` needs:

```python
paths = list_files()                       # ["a.csv", "b.csv"]
full = paths.map(lambda p: f"gs://raw/{p}")  # ["gs://raw/a.csv", "gs://raw/b.csv"]
process.expand(name=full)
```

It's the run-time equivalent of a list comprehension on an XCom, evaluated per
element when the mapping expands. No extra task node appears in the graph.

---

## 7. Reduce — collecting all mapped outputs

The fan-in is automatic: a **downstream, non-mapped** task that consumes a mapped
task's output receives the **full list** of every instance's return value:

```python
counts = process.expand(name=files)   # mapped; each returns an int

@task
def total(values: list[int]) -> int:  # NOT mapped -> gets [r0, r1, r2, ...]
    return sum(values)

total(counts)
```

`total` runs once, after all `process[i]` finish, with a list of their results.
That's the reduce step — no special API, just consume a mapped XComArg from an
unmapped task.

---

## 8. Guardrails — this fans out real work

Each mapped instance is a real scheduled TaskInstance. Fan-out that's unbounded or
huge can melt the scheduler and the metadata DB. Limits:

- **`max_map_length`** (core config, default **1024**) — hard ceiling on instances
  from one `.expand()`. Exceed it and the run **fails loudly** instead of quietly
  creating a scheduler incident. Raise deliberately, not by accident.
- **`max_active_tis_per_dag`** (on the task) — cap how many mapped instances of
  this task run **concurrently** (throttle, without shrinking the total count).
- **`max_active_tis_per_dagrun`** — same, but per DAG run.

If the list comes from an external source, **bound it** (a `LIMIT`, a slice)
before it reaches `.expand()`.

---

## 9. Complete runnable reference DAG

Self-contained (no providers) so it runs anywhere. Shows expand + partial + a
reduce. Note the **naming**: the Python function is `count_rows`, its `task_id` is
the *different* string `"row_count"`, so it's unambiguous that `.partial`/`.expand`
are called on the **function object**, and `"row_count"` is just the id.

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
        # source_zone is constant (partial); filename varies (expand)
        fake_sizes = {"orders.csv": 1200, "users.csv": 300, "products.csv": 80}
        print(f"counting {source_zone}/{filename}")
        return fake_sizes[filename]

    @task
    def total_rows(counts: list[int]) -> int:      # reduce: gets the whole list
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
`total_rows` runs once with `[1200, 300, 80]`.

---

## 10. Build spec — your challenge (no solution)

**File:** `dags/task-4/04_dynamic_mapping.py`  ·  **dag_id:** `s04_dynamic_mapping`

Build a DAG that fans out work over a **run-time-determined** list and reduces the
results — the count of parallel instances must be decided during the run, not
hard-coded.

**The problem:**

- A first task produces a **list** at run time (its length is not known when the
  DAG is written) — e.g. a list of "shards", filenames, or region codes.
- A second task is **mapped** over that list (one instance per element) and does
  per-element work returning a value. It must also take at least one **constant**
  argument that is the same for every instance.
- A third, **unmapped** task **reduces** all the mapped results into a single
  output and logs it.

**Constraints:**

- The mapped count comes from the first task's output — no literal list passed
  straight to `.expand()`, and no Python loop creating tasks in top-level code.
- Constant args use `.partial()`; the varying arg uses `.expand()`.
- Keep every Python function/variable name **distinct** from its `task_id` string.
- Passes the integrity gates: `tags`, real `owner`, `retries >= 1`.

**Acceptance criteria:**

- `python dags/task-4/04_dynamic_mapping.py` parses (prints nothing).
- `airflow dags test s04_dynamic_mapping 2026-01-01` runs green.
- The UI shows the mapped task as indexed instances `name[0]`, `name[1]`, … whose
  count equals the length of the first task's output.
- The reduce task runs **once** and receives the list of all mapped results.
- `python -m pytest tests/ -v` stays green.

**Stretch (optional):** make the first task return a list of **dicts** and use
`.expand_kwargs()` instead of `.partial().expand()`; or combine two lists
pairwise with `.zip()` and confirm you get N instances, not N×M.

**Nudge (only if stuck):** the shape is exactly the §9 reference —
`producer → mapped_worker.partial(const=...).expand(var=producer()) → reducer`.
Vary *what* the producer returns and *what* the worker computes.

---

## 11. Production tip — bound your fan-out, and make it idempotent

Dynamic mapping is where a DAG quietly turns into a scheduler incident. Two habits
separate safe fan-out from the kind that melts a cluster:

- **Always bound the source list.** The number of instances should have a known
  ceiling — a `LIMIT` on the query, a slice, a validated input. Leave
  `max_map_length` (1024) as the backstop, not the plan; a run that legitimately
  needs 5,000 shards should be redesigned (batch them), not have the limit bumped.
  Use `max_active_tis_per_dag` to throttle concurrency so 500 instances don't all
  hit a downstream API at once.
- **Each mapped instance must be idempotent and self-contained.** Instance `[k]`
  should depend only on *its* element, never on sibling instances or shared
  mutable state, and re-running it must be safe (write to a per-element,
  partition-scoped destination). That's what makes a single failed `name[37]`
  retry cleanly without corrupting the other 200.

---

## 12. Verify + commit

```bash
python dags/task-4/04_dynamic_mapping.py
airflow dags test s04_dynamic_mapping 2026-01-01
python -m pytest tests/ -v
git add -A && git commit -m "session 04: dynamic task mapping" && git push
```

Done when the DAG runs green, the UI shows the run-time-sized set of mapped
instances, and the reduce task collects them. Tick **04** in `README.md`.

Sources:
[Dynamic Task Mapping — Airflow 3.3 docs](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/dynamic-task-mapping.html),
[Task SDK — Dynamic Task Mapping](https://airflow.apache.org/docs/task-sdk/stable/dynamic-task-mapping.html)
