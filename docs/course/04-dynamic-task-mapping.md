# Session 04 · W2 Sun — Dynamic Task Mapping

**One line:** dynamic task mapping is a **`for` loop that Airflow runs for you at
run time**, where each pass of the loop becomes its own separate task.

You will learn `.expand`, `.partial`, `.expand_kwargs`, `.map`, `.zip`, how each
task is numbered (`map_index`), and how one final task adds up all the results.

---

## 0. The problem it solves

### The analogy: a pizza shop

You run a pizza shop. During the day, orders come in. **You don't know how many
orders you'll get today** — maybe 3, maybe 30. You find out only as the day goes.

You have one instruction: **"make one pizza per order."** You don't write a
separate instruction for order #1, order #2, order #3. You write it once, and it
runs once per order.

That is dynamic task mapping:

| Pizza shop | Airflow |
|---|---|
| Today's list of orders | a **list** produced during the run |
| The instruction "make one pizza" | one **mapped task** |
| Actually making pizza for order #2 | one **copy** of the task, `make_pizza[1]` |
| Counting total pizzas at closing | the **reduce** task |

### Why not just a normal loop?

As a data engineer your instinct is a `for` loop:

```python
for order in todays_orders:      # WRONG here
    make_pizza(order)
```

In Airflow this does **not** work, because of something from Session 01: code at
the top of the file runs at **parse time**, not run time. Airflow re-reads
(parses) the DAG file constantly — roughly every 30 seconds — just to see what the
DAG looks like. That is *not* the moment your pipeline runs. So this loop would ask
"how many orders?" at the wrong time, over and over, and the DAG's shape would keep
changing on its own.

You need Airflow to build the loop **when the pipeline actually runs and the real
list is known**. That is exactly what `.expand()` does:

```python
make_pizza.expand(order=todays_orders)   # Airflow runs the loop at run time
```

You write it once. When a run starts, Airflow looks at the list, and makes one task
per item — 3 tasks on a 3-order day, 30 on a 30-order day.

### What it looks like

```
             ┌─ make_pizza[0]  (order a)
orders ─────▶├─ make_pizza[1]  (order b)   ──▶  total (one task, gets [r0, r1, r2])
             └─ make_pizza[2]  (order c)
   the list        one task per order              add up all results
```

Each pizza task is a **normal, separate task**. It can succeed, fail, and retry on
its own. In the UI they show as `make_pizza[0]`, `make_pizza[1]`, `make_pizza[2]`.
The number in brackets is the **`map_index`**, starting at 0.

---

## 1. `.expand()` — run the loop (one task per item)

`.expand()` is the loop. You call it on a task and give it the argument that
changes each pass. Airflow makes **one copy of the task per item** in the list.

```python
@task
def process(name: str) -> str:
    return name.upper()

process.expand(name=["a", "b", "c"])   # -> process[0]=A, process[1]=B, process[2]=C
```

- The keyword (`name=`) must be the **name of a parameter** of the function.
- Each item in the list becomes that parameter's value for one copy.
- `.expand()` takes a **list**, a **dict**, or **another task's output**. It does
  **not** take a plain constant — those go in `.partial()` (§2).

The real power: the list is usually **another task's output**, known only at run
time (your "today's orders"):

```python
orders = get_orders()                # returns e.g. ["a", "b"] during the run
process.expand(name=orders)          # number of copies = number of orders
```

---

## 2. `.partial()` — the things that are the same every time

A mapped task usually has some arguments that **change each pass** and some that
are **the same for every pass**. Back to pizza: the *order* changes each time, but
the *shop address* and *oven temperature* are the same for all of them.

The same-for-all arguments go in `.partial()`:

```python
@task
def make_pizza(order: str, shop: str, oven_temp: int) -> int:
    ...

make_pizza.partial(shop="MG Road", oven_temp=250).expand(order=orders)
```

Every copy gets the same `shop` and `oven_temp`. Only `order` changes.

Simple rule: **`.partial()` = same for all. `.expand()` = one value per copy.** Any
argument not in `.expand()` must be in `.partial()` (or have a default), or parsing
fails.

---

## 3. Expanding over two lists = every combination (careful)

If you `.expand()` over **two** arguments, Airflow makes a copy for **every
combination**, not for pairs:

```python
add.expand(x=[1, 2], y=[10, 20])
# -> 4 copies: (1,10) (1,20) (2,10) (2,20)
```

Two lists of 100 items = 10,000 copies. If you want **pairs** instead (first with
first, second with second), use `.zip()` (§5) or `.expand_kwargs()` (§4) — not two
`.expand()` arguments.

---

## 4. `.expand_kwargs()` — hand each copy a full order slip

Sometimes each copy needs its own set of several values together (not every
combination). Pass a **list of dicts**. Each dict is the complete set of arguments
for one copy — like a full order slip:

```python
run_job.expand_kwargs([
    {"query": "SELECT 1", "location": "US"},
    {"query": "SELECT 2", "location": "EU"},
])
# -> 2 copies, each gets one whole dict as its arguments
```

`.expand()` changes one argument and makes every combination. `.expand_kwargs()`
sets the **whole** argument set for each copy.

---

## 5. `.zip()` — pair up two lists

To join two lists into pairs (like Python's `zip`), call `.zip()` on a task's
output. You get a list of pairs to expand over:

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

- It stops at the **shortest** list, like `zip`. Pass `fillvalue=` to change that.
- This gives pairs, avoiding the "every combination" blow-up from §3.

## 6. `.map()` — tweak each item before the loop

`.map()` runs a small function on **each item** of a list and gives back a new
list, without adding a task. Use it to reshape one task's output into the form the
next task needs:

```python
paths = list_files()                         # ["a.csv", "b.csv"]
full = paths.map(lambda p: f"gs://raw/{p}")  # ["gs://raw/a.csv", "gs://raw/b.csv"]
process.expand(name=full)
```

Think of it as a list comprehension on a value that only exists during the run. It
does not add a new box to the graph.

---

## 7. Reduce — one task that adds up all the results

The last step is automatic. If a **later task that is not mapped** takes the mapped
task's output as its input, it receives the **full list** of results from all
copies. (At closing time, you count *all* the pizzas.)

```python
counts = process.expand(name=orders)   # mapped; each copy returns a number

@task
def total(values: list[int]) -> int:   # not mapped -> gets [r0, r1, r2, ...]
    return sum(values)

total(counts)
```

`total` runs **once**, after every `process[i]` finishes, with a list of their
results. There is no special function for this — you just feed a mapped task's
output into a normal task.

---

## 8. Guardrails — this makes real work

Every copy is a real task the scheduler must track. A giant or unbounded list can
overload the scheduler and the database. Controls:

- **`max_map_length`** (a core setting, default **1024**) — the hard limit on how
  many copies one `.expand()` can make. Go over it and the run **fails with a clear
  error** instead of quietly overloading everything. Raise it only on purpose.
- **`max_active_tis_per_dag`** (set on the task) — how many copies run **at the same
  time**. Limits concurrency without changing the total count.
- **`max_active_tis_per_dagrun`** — same idea, counted per DAG run.

If the list comes from outside (a query, an API), **put a limit on it** (a `LIMIT`
or a slice) before it reaches `.expand()`.

---

## 9. Complete runnable reference DAG

Self-contained (no providers), so it runs anywhere. It shows `.expand` +
`.partial` + a reduce.

Notice the **naming**: the Python function is `count_rows`, but its `task_id` is a
**different** string, `"row_count"`. Keeping them different makes it obvious that
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
        # in real life this reads a bucket/dir AT RUN TIME (this is "today's orders")
        return ["orders.csv", "users.csv", "products.csv"]

    @task(task_id="row_count")            # task_id != function name (count_rows)
    def count_rows(filename: str, source_zone: str) -> int:
        # source_zone is the same for all copies (partial); filename changes (expand)
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
`total_rows` runs once with the list `[1200, 300, 80]`.

---

## 10. Build spec — your challenge (no solution)

**File:** `dags/task-4/04_dynamic_mapping.py`  ·  **dag_id:** `s04_dynamic_mapping`

Build a DAG that spreads work over a list whose length is known only during the
run, then adds up the results. The number of parallel copies must come from the
run, not be hard-coded. (Your own "pizza shop".)

**The problem:**

- A first task returns a **list** during the run. Its length is not known when you
  write the DAG (for example a list of shards, filenames, or region codes).
- A second task is **mapped** over that list — one copy per item. Each copy does
  some work and returns a value. It must also take at least one argument that is
  the **same for every copy**.
- A third task is **not mapped**. It takes all the results from the second task,
  combines them into one value, and logs it.

**Constraints:**

- The number of copies must come from the first task's output. Do not pass a
  hard-coded list to `.expand()`, and do not build tasks with a top-level `for`
  loop.
- Put the same-for-all arguments in `.partial()` and the changing one in
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
pairs with `.zip()` and check you get N copies, not N×M.

**Nudge (only if stuck):** same shape as the §9 reference —
`producer → worker.partial(const=...).expand(var=producer()) → reducer`. Change
*what* the producer returns and *what* the worker computes.

---

## 11. Production tip — cap the loop, and make each copy safe to re-run

Dynamic mapping is a common way to accidentally overload a cluster. Two habits keep
it safe:

- **Always cap the source list.** The number of copies should have a known upper
  limit: a `LIMIT` on the query, a slice, or a checked input. Treat `max_map_length`
  (1024) as a safety net, not a plan. If a run truly needs 5,000 copies, redesign it
  to work in batches instead of raising the limit. Use `max_active_tis_per_dag` so
  that, say, 500 copies don't all hit the same downstream API at once.
- **Each copy must stand alone and be safe to re-run.** Copy `[k]` should use only
  its own item — never the results of other copies or shared changing state. And
  re-running it must be safe, so write its output to a spot tied to that item (a
  per-item or per-partition destination). That is what lets one failed `name[37]`
  retry cleanly without breaking the other copies.

---

## 12. Verify + commit

```bash
python dags/task-4/04_dynamic_mapping.py
airflow dags test s04_dynamic_mapping 2026-01-01
python -m pytest tests/ -v
git add -A && git commit -m "session 04: dynamic task mapping" && git push
```

Done when the DAG runs green, the UI shows the run-time number of copies, and the
third task adds them all up. Tick **04** in `README.md`.

Sources:
[Dynamic Task Mapping — Airflow 3.3 docs](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/dynamic-task-mapping.html),
[Create dynamic Airflow tasks — Astronomer](https://www.astronomer.io/docs/learn/dynamic-tasks)
