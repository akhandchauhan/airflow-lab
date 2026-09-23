# Session 11 · Timetables

**Goal:** understand the object that *actually* decides when your DAG runs — the **Timetable** — and the one distinction that trips everyone: `CronTriggerTimetable` and `CronDataIntervalTimetable` fire at the **same wall-clock time** but disagree about **what data window each run represents**. Then learn to write a custom `Timetable` subclass for a cadence cron simply cannot express (business-days-only). The idea to lock in: every value you put in `schedule=` — a cron string, a `timedelta`, `@daily` — is just shorthand that Airflow resolves into a Timetable object, and the Timetable is what answers "when is the next run, and which interval does it own." Plain DAGs, no BigQuery. *(Phase B — scheduling & data-awareness.)*

---

## 1. What a Timetable is

A **Timetable** is the object behind `schedule=`. When you write `schedule="@daily"`, Airflow doesn't store the string — it constructs a Timetable and asks *that* two questions on every scheduler loop: **"given the last run's interval, when is the next run and what window does it cover?"** (`next_dagrun_info`) and, when someone clicks Trigger manually, **"what window should this off-schedule run represent?"** (`infer_manual_data_interval`).

The running analogy: a Timetable is a **train schedule board**. The cron string is just one way to fill the board in. But two boards can post the *same departure time* and still mean different things — one board tells you "the 06:00 train leaves at 06:00" (you care about the departure), the other tells you "the 06:00 train carries **yesterday's** cargo" (you care about the load it represents). Same clock, different meaning of the trip. That's the entire `CronTrigger` vs `CronDataInterval` split.

Everything below is detail on that board.

---

## 2. The distinction: trigger time vs data interval

Both timetables fire at **identical wall-clock times**. The difference is purely in what `data_interval_start` / `data_interval_end` — and therefore the `run_id`/label — mean.

| | `CronTriggerTimetable` | `CronDataIntervalTimetable` |
|---|---|---|
| When it fires | at the cron time | at the cron time |
| `data_interval_start` | = the trigger time | = the **previous** trigger time |
| `data_interval_end` | = the trigger time (**same as start**) | = the trigger time |
| So the interval is | a **point** (start == end) | a **span** (the gap between two fires) |
| Run label reflects | the moment it fires | the **start** of the window it covers |
| Use when | you just want "run at 06:00" | each run **processes the window since the last run** |

The canonical scenario the docs use — **daily at midnight, DAG enabled at 3PM on Jan 31**:

- **`CronTriggerTimetable`** — the next midnight is Feb 1 00:00, so it fires **Feb 1**; the `run_id` shows **Feb 1**; `data_interval_start == data_interval_end ==` Feb 1 00:00. There is no "window"; the run is a point in time.
- **`CronDataIntervalTimetable`** — the most recent completed interval is Jan 31 → Feb 1, and it already closed conceptually, so it fires **immediately** for the **Jan 31** interval; the `run_id` shows **Jan 31** (the interval's start); the window is Jan 31 00:00 → Feb 1 00:00.

So the same schedule, on the same clock, hands one run a **label of Feb 1 and no window**, and the other a **label of Jan 31 and a full day's window**. If your task filters data on `data_interval_start`, that one-day label shift is the difference between processing the right day and the wrong one.

**Rule of thumb:** if the task cares about a *time window of data* (Session 09's incremental pattern, Session 10's idempotent backfill), you want a **data-interval** timetable so `[data_interval_start, data_interval_end)` is a real span. If the task just needs to *fire at a time* and doesn't slice data by interval, `CronTriggerTimetable` is simpler and its label matches the wall clock — no "why does my midnight run say yesterday" confusion.

---

## 3. The Airflow 3 default — a bare cron string is a *trigger* timetable

This bites people migrating mental models: in **Airflow 3**, a plain cron string in `schedule=` resolves to `CronTriggerTimetable`, **not** the data-interval one.

- The switch is the config `[scheduler] create_cron_data_intervals`. It **defaults to `False` in Airflow 3** (it was `True` in 2.x). "Flipping `create_cron_data_intervals` changes how every DAG with a bare cron string in `schedule=` resolves its timetable."
- `False` → bare cron ⇒ `CronTriggerTimetable` (point-in-time, `start == end`).
- To get real windows from a cron string, either flip that config **or** — better, explicit and local — pass the timetable object yourself: `schedule=CronDataIntervalTimetable("0 0 * * *", timezone="UTC")`.

Don't rely on the global flag; if a DAG needs data intervals, name the data-interval timetable in that DAG. Local and obvious beats a cluster-wide config that silently changes every cron DAG's semantics.

---

## 4. Built-in timetables and their import paths

Everything you pass to `schedule=` maps to one of these. You rarely import them for the common cases (the string/`timedelta` shorthand does it), but you import them when you want the *other* semantics or extra options.

| Class | Import path | What it is |
|---|---|---|
| `CronTriggerTimetable` | `airflow.timetables.trigger` | cron, point-in-time (`start == end`); the Airflow 3 default for bare cron strings |
| `MultipleCronTriggerTimetable` | `airflow.timetables.trigger` | several cron expressions OR'd together (fire on any of them) |
| `DeltaTriggerTimetable` | `airflow.timetables.trigger` | fire once a `timedelta` has elapsed; point-in-time |
| `CronDataIntervalTimetable` | `airflow.sdk` | cron, but each run owns the **span** between two fires |
| `DeltaDataIntervalTimetable` | `airflow.sdk` | `timedelta` cadence with a real interval; not clock-aligned |
| `EventsTimetable` | `airflow.timetables.events` | run at an explicit **list of datetimes** you supply |
| `AssetOrTimeSchedule` | `airflow.timetables.assets` | run on an asset event **or** on a time schedule (Session 12's floor) |

The shorthand ↔ object mapping worth memorizing:

- `schedule="@daily"` / any bare cron ⇒ `CronTriggerTimetable` (Airflow 3 default).
- `schedule=timedelta(hours=6)` ⇒ `DeltaDataIntervalTimetable`.
- `schedule=None` ⇒ no timetable (manual/asset only).

Common presets (`@once`, `@continuous`, `@hourly`, `@daily`, `@weekly`, `@monthly`, `@yearly`) are cron shorthands; `@once` runs a single time and `@continuous` re-triggers as soon as the previous run finishes (one at a time).

---

## 5. When cron isn't enough — a custom Timetable

Cron can say "07:00 Monday–Friday" (`0 7 * * 1-5`). It **cannot** say "every business day, *skipping the company holiday list*," or "the last business day of the month," or "07:00 but shift to the next weekday if it lands on a weekend." When the cadence is a rule cron can't spell, you subclass `Timetable`.

You subclass `airflow.timetables.base.Timetable` and implement **two** methods:

```python
from airflow.timetables.base import DagRunInfo, DataInterval, Timetable, TimeRestriction

class AfterWorkdayTimetable(Timetable):

    def next_dagrun_info(              # ← THE MECHANIC: "when is the next scheduled run?"
        self,
        *,
        last_automated_data_interval: DataInterval | None,
        restriction: TimeRestriction,
    ) -> DagRunInfo | None:
        ...
        return DagRunInfo.interval(start=next_start, end=next_start.add(days=1))

    def infer_manual_data_interval(   # ← "someone clicked Trigger — what window is that run?"
        self, run_after: DateTime
    ) -> DataInterval:
        ...
        return DataInterval(start=start, end=start.add(days=1))
```

The pieces, exact names and meanings:

| Type | From | What it carries |
|---|---|---|
| `Timetable` | `airflow.timetables.base` | the base class you subclass |
| `TimeRestriction` | `airflow.timetables.base` | `earliest`, `latest` (from `start_date`/`end_date`), and `catchup` (bool) |
| `DataInterval` | `airflow.timetables.base` | a window: `.start`, `.end` (timezone-aware `pendulum` datetimes) |
| `DagRunInfo` | `airflow.timetables.base` | what `next_dagrun_info` returns; build with `DagRunInfo.interval(start=, end=)` |

- `next_dagrun_info` receives the **last run's interval** (or `None` on the very first run) and the `restriction`. Return a `DagRunInfo` to schedule that run, or `None` to stop (past `latest`, or nothing to do). It must honor `restriction.catchup`: when `False`, jump to the most recent valid slot instead of walking every missed one.
- `infer_manual_data_interval` is called when a human triggers off-schedule — it reverse-computes which window that manual run should represent.
- All datetimes must be **timezone-aware** and use `pendulum` types.

**Registering it:** a custom timetable must be importable and registered through a plugin so the serializer knows it:

```python
from airflow.plugins_manager import AirflowPlugin

class WorkdayTimetablePlugin(AirflowPlugin):
    name = "workday_timetable_plugin"
    timetables = [AfterWorkdayTimetable]     # ← makes the class known to Airflow
```

**Parameterized timetables** (e.g. "run at a configurable time") must also implement `serialize()` / `deserialize()` and a `summary` property so Airflow can store and display them:

```python
def serialize(self) -> dict:
    return {"schedule_at": self._schedule_at.isoformat()}

@classmethod
def deserialize(cls, value: dict) -> "Timetable":
    return cls(Time.fromisoformat(value["schedule_at"]))
```

---

## 6. A complete runnable DAG (your reference)

The cleanest thing to *see* is the trigger-vs-interval difference. This DAG uses `CronTriggerTimetable` and prints its interval — you'll watch `data_interval_start == data_interval_end`, the signature of a point-in-time timetable. Plain, no BigQuery.

```python
from __future__ import annotations

import pendulum
from airflow.sdk import dag, get_current_context, task
from airflow.timetables.trigger import CronTriggerTimetable


@dag(
    dag_id="s11_examples",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=CronTriggerTimetable("0 6 * * *", timezone="UTC"),   # fires 06:00 daily
    catchup=False,
    tags=["session-11", "timetables"],
    default_args={"owner": "akhand", "retries": 1},
)
def pipeline():

    @task
    def show_interval() -> None:
        ctx = get_current_context()
        start = ctx["data_interval_start"]
        end = ctx["data_interval_end"]
        # CronTriggerTimetable: start == end (a point, not a span).
        print(f"fired for {ctx['logical_date']}")
        print(f"data_interval_start = {start}")
        print(f"data_interval_end   = {end}")
        print(f"is a point in time  = {start == end}")   # → True here

    show_interval()


pipeline()
```

```bash
python dags/s11/s11_examples.py                  # parses (prints nothing)
airflow dags test s11_examples 2026-01-01        # prints start == end == the fire time
```

You'll see `is a point in time = True`. Now mentally swap the schedule for `CronDataIntervalTimetable("0 6 * * *", timezone="UTC")` (import from `airflow.sdk`): the same 06:00 fire would instead show a **24-hour span** with `start` a day before `end`, and `is a point in time = False`. Same clock, different meaning — exactly §2.

---

## 7. Build spec — your challenge (no solution)

**File:** `dags/s11/s11_assignment.py`  ·  **dag_id:** `s11_assignment`

Build a DAG that runs **only on business days** — a cadence a single cron string can't express cleanly — using a **custom `Timetable`**.

**The problem:**

- Write a `Timetable` subclass (`airflow.timetables.base.Timetable`) that schedules **one run per weekday** (Mon–Fri), skipping Saturday and Sunday. Implement `next_dagrun_info` and `infer_manual_data_interval`; return `DagRunInfo.interval(...)`; honor `restriction.earliest` / `restriction.latest` / `restriction.catchup`.
- Register it via an `AirflowPlugin` with a `timetables = [...]` list so Airflow can load and serialize it.
- Apply it to the DAG with `schedule=YourTimetable()`. One `@task` prints the run's `data_interval_start` and its weekday name so you can confirm no run lands on a weekend.

**Constraints:**

- Plain TaskFlow, no BigQuery.
- The cadence lives in the **timetable**, not in a task that skips itself — the scheduler should simply never create a weekend run.
- All datetimes timezone-aware (`pendulum`). Passes the integrity gates: `tags`, real `owner`, `retries >= 1`.

**Acceptance criteria:**

- `python dags/s11/s11_assignment.py` parses (prints nothing).
- `airflow dags test s11_assignment 2026-01-02` (a Friday) runs and prints a weekday.
- Across a Fri→Mon span, the scheduler produces a run for Friday and Monday but **none** for Saturday or Sunday.
- `python -m pytest tests/ -v` stays green.

**One nudge (only if stuck):** the whole job of `next_dagrun_info` is to advance from the last interval's start to the next valid slot, then loop past any weekend before returning — the docs' `AfterWorkdayTimetable` example is the exact shape you're reproducing.

---

## 8. Production tip — the report that ran a day early

The bug that pages you at 2am: an analyst's daily aggregate started emitting numbers that were consistently **one day short** after a migration. Nothing in the query changed. The cause was the timetable: the DAG had `schedule="0 2 * * *"` and, on Airflow 3, a bare cron string is a **`CronTriggerTimetable`** — so `data_interval_start == data_interval_end == 02:00 today`. The task filtered `WHERE event_date >= data_interval_start`, which under the old `CronDataIntervalTimetable` had meant "yesterday 02:00 → today 02:00" (a full day) and now meant "today 02:00 → today 02:00" (**an empty window**). The clock was identical; the *interval semantics* silently flipped.

- **Know which timetable your cron string became.** In Airflow 3 a bare cron is trigger-based (`create_cron_data_intervals=False`). If your task slices data by `[data_interval_start, data_interval_end)`, pass `CronDataIntervalTimetable(...)` **explicitly** so the window is a real span — don't inherit it from a cluster config that can change under you.
- **Assert the window isn't a point.** In interval-processing tasks, a one-line guard — `assert data_interval_start < data_interval_end` — turns "silently empty results" into a loud failure you catch in testing, not in the dashboard.

---

## 9. Verify + commit

```bash
python dags/s11/s11_assignment.py
airflow dags test s11_assignment 2026-01-02
python -m pytest tests/ -v
git add -A && git commit -m "session 11: timetables" && git push
```

Done when the custom timetable produces weekday-only runs and never a weekend one. Tick Session 11 in `docs/course/README.md`.

**Pre-push habit:** `ruff check dags/ include/ tests/ --select E,F,AIR3 && python -m pytest tests/ -v` before every push — green locally means green CI.

Sources:
[Timetables & data intervals — Airflow 3.3](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/timetable.html),
[Customizing DAG scheduling with Timetables — Airflow 3.3](https://airflow.apache.org/docs/apache-airflow/stable/howto/timetable.html),
[DAG runs & catchup — Airflow 3.3](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dag-run.html)
