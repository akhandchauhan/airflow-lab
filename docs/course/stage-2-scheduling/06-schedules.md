# Session 06 · Schedules & Intervals

**Goal:** control **when** a DAG runs and **what time window** each run represents.
Understand `schedule`, `start_date`, and `catchup`, and the idea that trips everyone
up: a scheduled run covers a **data interval** and fires **after that interval ends**.
Plain DAGs, no BigQuery. *(Phase B — scheduling & data-awareness — starts here.)*

---

## 1. The three questions scheduling answers

| Question | Param | Example |
|---|---|---|
| When is the DAG allowed to start? | `start_date` | `pendulum.datetime(2026, 1, 1, tz="UTC")` |
| How often does it run? | `schedule` | `"@daily"`, `"0 6 * * *"`, `timedelta(hours=1)`, `None` |
| Run the periods before today too? | `catchup` | `False` (default in Airflow 3) |

`start_date` is **when the schedule begins counting**, not "run it now." A DAG with
`start_date` in the past and `catchup=False` runs on the **next** boundary, not for
all the missed ones.

---

## 2. `schedule` — the values it accepts

| Value | Meaning |
|---|---|
| cron string `"0 6 * * *"` | standard cron — here, 06:00 every day |
| cron preset `"@daily"` | shorthand: `@hourly`, `@daily`, `@weekly`, `@monthly`, `@yearly`, `@once` |
| `timedelta(hours=1)` | fixed gap between runs, relative to the last |
| `None` | never auto-runs — manual / API trigger only (what every session so far used) |
| `[Asset(...)]` | data-aware — run when an asset updates (Session 07+) |

`@daily` == `"0 0 * * *"` (midnight). Use `None` while developing; add a real schedule
when the DAG is ready to run itself.

---

## 3. The data interval — the part everyone gets wrong

Each scheduled run represents a **window of time**, the **data interval**, not the
instant it runs. And a run fires **after its window closes** — so it has complete data
for that window.

```
@daily DAG, the run labelled 2026-01-01:

  data_interval_start           data_interval_end / when it actually RUNS
  2026-01-01 00:00  ───────────────────► 2026-01-02 00:00
        └──────── the window this run is responsible for ────────┘
```

So the "Jan 1" run does **not** execute on Jan 1 — it executes just after midnight on
**Jan 2**, once Jan 1's data is fully in. Three context values name this:

| Context key | What it is |
|---|---|
| `data_interval_start` | start of the window (here, Jan 1 00:00) |
| `data_interval_end` | end of the window = roughly when the run fires (Jan 2 00:00) |
| `logical_date` | the run's timestamp/label (used in the `run_id`); for interval schedules = `data_interval_start` |
| `ds` | `logical_date` as a `YYYY-MM-DD` string (`"2026-01-01"`) |

**Process the window, not "now."** Your task should filter data to
`[data_interval_start, data_interval_end)` — never `datetime.now()` — so it produces
the same result whether it runs on time, retries, or is re-run months later.

> In Airflow 3, a **manually** or **asset**-triggered run can have `logical_date =
> None` (there's no interval). Code that reads it should handle that — prefer
> `data_interval_start` for window logic.

---

## 4. `catchup` — the switch that spawns a hundred runs

`catchup` decides what happens to the intervals **between `start_date` and now**.

| `catchup` | Behavior |
|---|---|
| `False` *(default)* | skip the backlog — only run from the next boundary forward |
| `True` | create a run for **every** missed interval since `start_date` |

Turn `catchup=True` on a DAG whose `start_date` is a year ago with `@daily`, and
Airflow tries to launch **365 runs** at once. That's occasionally what you want (a
first backfill), but flip it **on purpose** — it's the classic "why did my scheduler
melt" surprise. Default it `False`; backfill deliberately when you mean to (Session
09's backfill topic later).

---

## 5. Reading the interval in a task

```python
from airflow.sdk import task, get_current_context

@task
def show_interval() -> None:
    ctx = get_current_context()
    print(f"window {ctx['data_interval_start']} → {ctx['data_interval_end']}")
    print(f"ds = {ctx['ds']}")
```

Or with Jinja in a templated field: `{{ data_interval_start }}`, `{{ ds }}`.

---

## 6. A complete runnable DAG (your reference)

A daily DAG that prints its own window. Plain, no BigQuery.

```python
from __future__ import annotations

import pendulum
from airflow.sdk import dag, task, get_current_context


@dag(
    dag_id="s6_examples",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule="@daily",
    catchup=False,
    tags=["session-6", "scheduling"],
    default_args={"owner": "akhand", "retries": 1},
)
def pipeline():

    @task
    def show_interval() -> None:
        ctx = get_current_context()
        print(f"logical_date        = {ctx['logical_date']}")
        print(f"data_interval_start = {ctx['data_interval_start']}")
        print(f"data_interval_end   = {ctx['data_interval_end']}")
        print(f"ds                  = {ctx['ds']}")

    show_interval()


pipeline()
```

```bash
python dags/stage-2-scheduling/s6/s6_examples.py
airflow dags test s6_examples 2026-01-01
```

`dags test` runs the interval you name. You'll see `data_interval_start = 2026-01-01`,
`data_interval_end = 2026-01-02`, `ds = 2026-01-01` — the window this run owns. Run it
with `2026-01-02` and every value shifts by a day. That shift is what you filter your
data on.

---

## 7. Build spec — your challenge (no solution)

**File:** `dags/stage-2-scheduling/s6/s6_assignment.py`  ·  **dag_id:** `s6_assignment`

Build a scheduled DAG that reports its own processing window.

**The problem:**

- Schedule it **`@hourly`**, `start_date` at `2026-01-01`, `catchup=False`.
- One `@task` prints a line like
  `processing window 2026-01-01T00:00 → 2026-01-01T01:00` using
  `data_interval_start` / `data_interval_end` from the context.
- A second `@task` prints just `{{ ds }}` (via Jinja or the context) to show the
  short date label.

**Constraints:**

- Plain TaskFlow, no BigQuery.
- The window comes from the **data interval**, never `datetime.now()`.
- Passes the integrity gates: `tags`, real `owner`, `retries >= 1`.

**Acceptance criteria:**

- `python dags/stage-2-scheduling/s6/s6_assignment.py` parses (prints nothing).
- `airflow dags test s6_assignment 2026-01-01` prints an **hour-long** window.
- Run it for `2026-01-01` and then a different hour — the window shifts accordingly.
- `python -m pytest tests/ -v` stays green.

---

## 8. Production tip — idempotency lives and dies on the data interval

- **Filter on the interval, not the wall clock.** A task that reads
  `WHERE ts >= '{{ data_interval_start }}' AND ts < '{{ data_interval_end }}'` produces
  the **same** output whether it runs on time, retries an hour later, or is re-run next
  year. Swap in `datetime.now()` and every run — and every retry — reads a different
  slice. This is *the* rule that makes backfills and retries safe.
- **`catchup=True` is a loaded gun.** Point it at a year-old `start_date` and it fires
  a run per interval. Keep it `False` and backfill a specific range on purpose when you
  actually need history.

---

## 9. Verify + commit

```bash
python dags/stage-2-scheduling/s6/s6_assignment.py
airflow dags test s6_assignment 2026-01-01
python -m pytest tests/ -v
git add -A && git commit -m "session 09: schedules & intervals" && git push
```

Done when `dags test` prints the correct window and it shifts with the run date. Tick
Session 06 in `docs/course/README.md`.

**Pre-push habit:** `ruff check dags/ include/ tests/ --select E,F,AIR3 && python -m pytest tests/ -v`
before every push — green locally means green CI.

Sources:
[Timetables & data intervals — Airflow 3.3](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/timetable.html),
[DAG runs & catchup — Airflow 3.3](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/dag-run.html)
