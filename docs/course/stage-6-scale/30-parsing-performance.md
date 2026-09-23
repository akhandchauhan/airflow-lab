# Session 30 · Parsing performance & scheduler tuning

**Goal:** understand the single most common reason a big Airflow install feels sluggish — the **DAG processor re-runs the top level of every DAG file on a loop**, and any real work you left up there (an import of pandas, a call to a database, an API request to build a task list) is paid *again and again*, not once. This is the "top-level code trap." Once you see that a DAG file is parsed continuously — not just when it runs — the fixes fall out: keep the top level cheap, tune how often and how parallel the parsing is, run more than one scheduler for headroom, and measure parse time instead of guessing. **Plain-DAG session — no BigQuery, no connection** (R16): the lesson is about Python import time, so we keep the example dependency-free.

---

## 1. The thing nobody tells you: your DAG file runs constantly

Here is the mental model most people get wrong. They think a DAG file executes *when the DAG runs*. It doesn't. The **DAG processor** (a separate component, `airflow dag-processor`) re-reads and re-executes the *top level* of every file in `dags/` on a repeating interval — every `[dag_processor] min_file_process_interval` seconds (**default 30**) — forever, just to notice if you changed anything. Your task **bodies** run when the DAG runs; your file's **top level** runs on a treadmill in the background all day.

So this innocent-looking file is a slow bleed:

```python
import pandas as pd                                  # ← re-imported every parse
import requests                                      # ← re-imported every parse

CONFIG = requests.get("https://config.svc/dags").json()   # ← an HTTP CALL every 30s, forever

@task
def run() -> None:
    ...
```

That `requests.get` is not run once at deploy — it fires every parse cycle, per file, on every scheduler. Multiply by 500 DAG files and you have a machine doing hundreds of pointless HTTP calls a minute, a DAG processor that can't keep up, and a UI where edits take minutes to appear. As the docs put it, *"the Airflow scheduler executes the code outside the Operator's `execute` methods with the minimum interval of `min_file_process_interval` seconds"* — and *"top-level code also means any code that is used to determine default values of methods."*

**The analogy — a menu vs cooking.** Parsing a DAG file is the waiter *reading the menu* to know what dishes exist; running a task is the kitchen *cooking* one. The waiter re-reads the menu every 30 seconds. If you've hidden "drive to the market and buy fish" inside the *menu text*, the waiter does the shopping trip every single time he reads it. Ingredients belong in the kitchen (the task body), not printed in the menu (top level).

---

## 2. What counts as "top-level code"

Anything that executes at **import time** — i.e., runs when Python reads the file, before any task fires:

- module-level `import` of heavy libraries (`pandas`, `numpy`, `torch`, a cloud SDK);
- any module-level function *call* — `requests.get(...)`, `Variable.get(...)`, a DB query, reading a file, `datetime.now()`;
- **default argument values computed by a call** — `def f(x=expensive())` runs `expensive()` at parse;
- building a task list by looping over something you fetched at the top level.

What is **safe** at the top level: cheap literals, defining the `@dag`/`@task` functions, `pendulum.datetime(...)` for `start_date`, small constant lists. The rule of thumb: **the top level should only *declare structure*, never *do work*.**

---

## 3. The fix: push work down into tasks (and imports too)

Move every expensive thing inside a task body, where it runs **only when that task runs**, not on every parse:

```python
@task
def run() -> None:
    import pandas as pd                              # ← THE MECHANIC: heavy import INSIDE the task
    import requests

    config = requests.get("https://config.svc/dags").json()   # runs at run-time, once per run
    df = pd.DataFrame(config)
    ...
```

The docs are explicit: *"top-level imports might take surprisingly a lot of time … this can be easily avoided by converting them to local imports inside Python callables."* Same for I/O and config: if a task needs it, the task fetches it. If *structure* needs it (you're generating tasks from a list), pull that list from something already in memory — Airflow `Variable`/`Connection` access at top level is itself a DB hit per parse and should be avoided; prefer a static config or `os.environ`.

---

## 4. Tuning the DAG processor — the two knobs

Once your files are lean, two settings control the parsing loop:

| Config | Section | Default | What it does |
| --- | --- | --- | --- |
| `min_file_process_interval` | `[dag_processor]` | `30` | seconds between re-parses of a file. **Raise it** (e.g. 60–120) to cut CPU when you don't need edits reflected in seconds; the cost is slower pickup of DAG changes. |
| `parsing_processes` | `[scheduler]` | `2` | how many files the processor parses **in parallel**. **Raise it** on a multi-core box with many files so the whole `dags/` folder gets through a cycle faster. |

Related guards: `[dag_processor] dag_file_processor_timeout` (**default 50s**) kills a single file's parse if it hangs, and `[core] dagbag_import_timeout` (**default 30.0s**) bounds how long one file's import may take. If a file trips these, it shows up as an **import error** — which is the loud version of "a slow parse."

Set them via env vars, e.g. `AIRFLOW__DAG_PROCESSOR__MIN_FILE_PROCESS_INTERVAL=60` and `AIRFLOW__SCHEDULER__PARSING_PROCESSES=4`. Tune only *after* you've fixed top-level code — a faster loop over slow files is still slow.

---

## 5. Scheduler HA — run more than one

Airflow 3 supports **running multiple schedulers active-active**, *"both for performance reasons and for resiliency."* They coordinate through the metadata DB using **row-level locks** (`SELECT ... FOR UPDATE`), so only one scheduler is ever in the critical section that enforces limits — no double-scheduling. On **PostgreSQL 12+ or MySQL 8.0+**, the docs say *"you can start running as many copies of the scheduler as you like — there is no further set up or config options needed."* You just launch a second `airflow scheduler`. This buys you throughput (more parsing/scheduling capacity) and failover (one dies, the other keeps going) — the standard answer when one scheduler can't keep the queue warm.

---

## 6. How to measure parse time (don't guess)

**Per file, the fast way** — the docs' own advice: run the file directly and time it.

```bash
time python dags/stage-6-scale/s30/s30_examples.py     # wall-clock to import + build the DAG
```

Subtract ~0.07s of interpreter startup; compare the number **before and after** you move imports down, on the same machine. That single command is the whole feedback loop — if it drops from 3s to 0.1s, you fixed it.

**Fleet-wide:** the DAG processor emits `dag_processing.total_parse_time` (and per-file stats) as metrics, and the UI surfaces import errors and processing status. Watch `total_parse_time` climb as you add DAGs; a jump when you merge one file points straight at that file's top level.

---

## 7. Complete runnable reference DAG (plain — no BigQuery)

Two lessons in one file: a **cheap top level** (just structure), and the **expensive work pushed into the task body**. Time the parse to prove it's fast.

```python
# dags/stage-6-scale/s30/parsing_demo.py
from __future__ import annotations

import pendulum                       # cheap: safe at top level
from airflow.sdk import dag, task     # cheap: just declarations

# ✅ top level does NOTHING expensive — no imports of heavy libs, no I/O, no calls.


@dag(
    dag_id="s30_parsing_demo",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["session-30", "parsing"],
    default_args={"owner": "akhand", "retries": 1},
)
def pipeline():

    @task
    def crunch() -> int:
        # ← THE MECHANIC: the heavy import + work lives HERE, so it runs at run-time,
        #   not on every 30-second parse cycle.
        import time

        time.sleep(2)                 # stand-in for "expensive work" (a real import, an API call)
        total = sum(range(1_000_000))
        print(f"crunched → {total}")
        return total

    crunch()


pipeline()
```

```bash
time python dags/stage-6-scale/s30/s30_examples.py     # should be ~0.1s: top level is cheap
airflow dags test s30_parsing_demo 2026-01-01          # the 2s of work happens now, at run-time
```

The point to *feel*: `time python …` (a parse) is near-instant because nothing heavy is at the top level, while `dags test` (a run) takes ~2s because that's where the work moved. Now imagine the `time.sleep(2)` were an `import torch` left at module level — the parse would take 2s **every 30 seconds, forever**. That's the trap, and moving it into `crunch` is the fix.

---

## 8. Build spec — your challenge (no solution)

**File:** `dags/stage-6-scale/s30/s30_assignment.py` · **dag_id:** `s30_assignment`

Take a deliberately slow-parsing DAG and make it parse fast.

**The problem:**

- Start from a DAG that does expensive work at **top level** — simulate it with a module-level `time.sleep(1.5)` (or a heavy import) that runs on every parse.
- Measure the parse cost: `time python dags/stage-6-scale/s30/s30_assignment.py`.
- Refactor so **all** import-time work moves inside task bodies, and the top level only declares structure. Measure again and confirm the parse time dropped.

**Constraints:**

- Plain TaskFlow, `airflow.sdk` imports, **no BigQuery / no connection** (R16 plain session).
- Passes the integrity gates: `tags`, real `owner`, `retries >= 1`.

**Acceptance criteria:**

- `time python dags/stage-6-scale/s30/s30_assignment.py` is near-instant (the slow part is gone from the top level).
- `airflow dags test s30_assignment 2026-01-01` runs green, with the work now happening at run-time.
- `python -m pytest tests/ -v` stays green.

**One nudge (only if stuck):** the giveaway is *where* the slow line sits — if `time python thefile.py` is slow, the work is still at module level. Nothing that does I/O, sleeps, or imports a heavy library belongs outside a task function.

---

## 9. Production tip — the import that slowed every DAG at once

The bug that pages you at 2am: someone added `import tensorflow as tf` at the top of one DAG file — a ~2-second import — to use it in a single task. Tests passed, the DAG ran fine. But the DAG processor now paid that 2 seconds **every `min_file_process_interval`, on every scheduler**, and with a few hundred files already in the folder the processor fell behind its cycle. The symptom wasn't an error — it was the whole install going mushy: new runs starting late, the UI showing stale DAGs, edits taking minutes to appear. Nobody connected it to one import line. The habit that prevents it: **heavy imports and any I/O live inside task bodies, never at module level**, and you *prove* it with `time python thefile.py` before merging — a DAG file that takes more than a fraction of a second to import is a file that will quietly tax every parse cycle for as long as it's deployed.

Sources:
[Best Practices (top-level code, parse timing) — Airflow 3.3](https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html),
[DAG File Processing — Airflow 3.3](https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/dagfile-processing.html),
[Scheduler / running multiple schedulers — Airflow 3.3](https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/scheduler.html),
[Configuration Reference — Airflow 3.3](https://airflow.apache.org/docs/apache-airflow/stable/configurations-ref.html)
