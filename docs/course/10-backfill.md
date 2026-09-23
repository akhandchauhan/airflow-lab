# Session 10 · Backfill

**Goal:** run a DAG for a **range of past dates on purpose** — filling history you missed, or reprocessing a window after you fixed a bug — using Airflow 3's **native backfill** (`airflow backfill create`), and understand why this is a *different tool* from `catchup`. The one idea to lock in: backfill is an explicit, bounded, user-launched job that creates one run per interval in a date range you name, and it is only safe because every run processes **its own data interval window**, never `datetime.now()`. Get idempotency right and a backfill you re-run three times produces the identical result three times. Plain DAGs, no BigQuery. *(Phase B — scheduling & data-awareness.)*

---

## 1. What backfill actually is

Backfill is when you **create runs for past dates of a DAG**. You hand Airflow a DAG id, a start date, and an end date, and it creates runs across that range **according to the DAG's schedule** — one run per interval the schedule would have produced.

The running analogy for the whole session: you went on holiday and the newspaper kept coming, but nobody read it. **Backfill is you sitting down and deliberately reading the specific week of back-issues you care about** — you name the dates, you control the pace, you can re-read a day you skimmed badly. It is a decision you make, not something that happens to you.

That "decision you make" is the whole point, and it is what separates it from the thing people confuse it with.

---

## 2. Backfill vs `catchup` — same-looking, different tools

Both end up creating runs for past intervals, so people treat them as synonyms. They are not. They differ on **who starts it**, **what bounds it**, and **whether you can control reprocessing**.

| | `catchup` | Backfill |
|---|---|---|
| Who triggers it | the **scheduler**, automatically | **you**, explicitly (CLI / UI / REST) |
| When | on unpause / first deploy, if `catchup=True` | whenever you run `airflow backfill create` |
| Bounds | everything from `start_date` to now | the exact `--from-date` → `--to-date` you name |
| Reprocess existing runs? | no — it only fills *missing* intervals | yes — `--reprocess-behavior` controls it |
| Is it a first-class object? | no, it's a DAG flag | **yes** — a Backfill job you can watch, pause, cancel |

`catchup` is the paper boy dumping the whole pile of unread papers on your porch the moment you get home (Session 09's "loaded gun"). Backfill is you walking to the archive and pulling exactly the issues from March 3rd to March 9th. One is automatic and unbounded; the other is manual, bounded, and re-runnable.

> In Airflow 3, backfill is **native**: it's a real Backfill entity created through `airflow backfill create`, the REST API, or the UI — not the old worker-side `airflow dags backfill` command. It runs through the normal scheduler and shows up in the UI like any other run, so `--max-active-runs`, retries, and pools all apply.

---

## 3. `airflow backfill create` — the command

```bash
airflow backfill create \
  --dag-id s10_examples \
  --from-date 2026-01-01 \
  --to-date 2026-01-07 \
  --reprocess-behavior none \
  --max-active-runs 3
```

The flags, exact names from the CLI reference:

| Flag | What it does |
|---|---|
| `--dag-id` | which DAG to backfill (required) |
| `--from-date` | start of the window, **inclusive** (required) |
| `--to-date` | end of the window, **inclusive** (required) |
| `--reprocess-behavior {none,completed,failed}` | how to treat intervals that already have a run (see §4); default `none` |
| `--max-active-runs` | how many backfill runs may execute **concurrently** — this is your throttle |
| `--run-backwards` | process from the **most recent** logical date first instead of oldest-first |
| `--dag-run-conf` | a JSON conf dict passed to every run in the backfill |
| `--dry-run` | show what would be created without creating anything |
| `--run-on-latest-version` / `--no-run-on-latest-version` | which bundle (code) version the backfill runs against (experimental) |

`--max-active-runs` is the flag that saves you: a 365-day backfill with `--max-active-runs 3` trickles three runs at a time through the scheduler instead of stampeding it. This is the deliberate, throttled version of the melt-your-scheduler problem `catchup=True` causes.

**UI equivalent:** in the DAG's page, **Trigger → Backfill** opens a form with the same fields — date range, reprocess behavior, max active runs, and run backwards. Same job, no terminal.

---

## 4. `--reprocess-behavior` — the three modes

This flag decides what happens when the interval you're backfilling **already has a run**. It is the reason backfill can *reprocess*, not just *fill gaps*.

| Value | Rule (exact) |
|---|---|
| `none` *(default)* | "if there's already a run for this logical date, do not create another, no matter the state" — pure gap-fill |
| `failed` | "if a run exists, if the state is failed, create a new run for this date" — retry only the broken ones |
| `completed` | "if a run exists, if the state is completed **or** failed, create a new run for this date" — redo everything in the range |

Reach for each one deliberately:

- **`none`** — history has holes (the DAG was paused for a week); you only want to fill the missing days, leaving good runs untouched.
- **`failed`** — a downstream API was flaky Tuesday; re-run only the days that failed.
- **`completed`** — you fixed a bug in the transformation logic; every day in the range is now *wrong* and must be recomputed from scratch.

**One hard guardrail across all three:** "If the latest run is still running or is queued, we do not create another run, no matter the chosen reprocessing behavior." Backfill never double-launches an interval that is still in flight — so re-issuing the command while a backfill is running won't clone live runs.

---

## 5. Idempotency — why a backfill must be safe to re-run

`--reprocess-behavior completed` will *re-run a day that already succeeded*. If running it a second time produces a **different** result than the first, your backfill is not a fix — it's a new bug that only shows up in reprocessed data. So the non-negotiable rule:

**A task must produce the identical output for a given interval every time it runs — on schedule, on retry, or in a backfill months later.**

The single thing that breaks this is reading the wall clock. Consider the difference:

```python
# ✗ NOT idempotent — reads "now", so a backfill of Jan 3 done today grabs TODAY's data
rows = fetch(since=datetime.now() - timedelta(days=1))     # ← the bug that ruins backfills

# ✓ Idempotent — reads the run's OWN window, identical no matter when it executes
ctx = get_current_context()
rows = fetch(start=ctx["data_interval_start"], end=ctx["data_interval_end"])  # ← THE MECHANIC
```

The backfilled run for Jan 3 carries `data_interval_start = 2026-01-03` and `data_interval_end = 2026-01-04` regardless of whether you run it on Jan 4 or in September. Filter on those, write to a partition/table keyed by the interval (an overwrite of that partition, not an append), and re-running is a no-op that lands the same bytes. This is Session 09's data-interval lesson turned into the thing that makes backfill actually usable — the window travels with the run, the clock does not.

> Idempotency isn't only about the *read* window. The **write** must also be re-run-safe: `MERGE`/overwrite the interval's partition rather than blind `INSERT`, or a `completed` backfill doubles every row it touches.

---

## 6. A complete runnable DAG (your reference)

A daily DAG whose one task prints the window it is responsible for. Run it once with `dags test`, then backfill a week and watch seven windows scroll by — each printing its *own* dates, none printing "today." Plain, no BigQuery.

```python
from __future__ import annotations

import pendulum
from airflow.sdk import dag, get_current_context, task


@dag(
    dag_id="s10_examples",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule="@daily",
    catchup=False,                       # scheduler won't auto-fill; we backfill on purpose
    tags=["session-10", "backfill"],
    default_args={"owner": "akhand", "retries": 1},
)
def pipeline():

    @task
    def process_window() -> str:
        ctx = get_current_context()
        start = ctx["data_interval_start"]
        end = ctx["data_interval_end"]
        # Everything downstream keys off [start, end) — never datetime.now().
        print(f"processing window {start} → {end}  (idempotent: same in/out every run)")
        return f"{start.to_date_string()}..{end.to_date_string()}"

    process_window()


pipeline()
```

```bash
python dags/s10/s10_examples.py                 # parses (prints nothing)
airflow dags test s10_examples 2026-01-01       # one run: window 2026-01-01 → 2026-01-02

# backfill a whole week, three runs at a time, gap-fill only:
airflow backfill create \
  --dag-id s10_examples \
  --from-date 2026-01-01 \
  --to-date 2026-01-07 \
  --reprocess-behavior none \
  --max-active-runs 3
```

You'll get seven runs, one per day, each printing **its own** window. Re-run the same `backfill create` with `--reprocess-behavior none` and nothing new is created — the intervals already have runs. Switch to `--reprocess-behavior completed` and all seven are recomputed; because the task keys off the interval, every recomputed run prints the identical window it did the first time. That equivalence *is* idempotency, made visible.

---

## 7. Build spec — your challenge (no solution)

**File:** `dags/s10/s10_assignment.py`  ·  **dag_id:** `s10_assignment`

Build a daily DAG that is **safe to backfill and reprocess**, then prove it by backfilling a range twice.

**The problem:**

- A `@daily` DAG, `start_date` `2026-01-01`, `catchup=False`.
- One `@task` that computes a value **derived only from its data interval** — e.g. the interval's `data_interval_start` day-of-year, or a deterministic hash of `ds` — and logs it. The value must depend on **nothing** but the run's own window (no `datetime.now()`, no random, no external mutable state).
- A second `@task` that logs whether this run *would* overwrite vs append, to make the write-safety point to yourself.

**Constraints:**

- Plain TaskFlow, no BigQuery.
- The processed window comes from the **data interval**, never the wall clock.
- Passes the integrity gates: `tags`, real `owner`, `retries >= 1`.

**Acceptance criteria:**

- `python dags/s10/s10_assignment.py` parses (prints nothing).
- `airflow dags test s10_assignment 2026-01-03` logs a value tied to Jan 3.
- `airflow backfill create --dag-id s10_assignment --from-date 2026-01-01 --to-date 2026-01-05 --reprocess-behavior none` creates the missing runs.
- Re-running that same command creates **nothing new** (they exist); switching to `--reprocess-behavior completed` recomputes all five and each logs the **identical** value it logged before.
- `python -m pytest tests/ -v` stays green.

**One nudge (only if stuck):** the value must be a pure function of the interval — if `f(2026-01-03)` ever prints two different numbers, your task isn't idempotent and no `--reprocess-behavior` will save it.

---

## 8. Production tip — the backfill that doubled the numbers

The bug that pages you at 2am: a teammate found the revenue mart was undercounting for the first week of the quarter, so they fixed the query and ran `airflow backfill create --reprocess-behavior completed` over that week to repair it. The dashboard **doubled** overnight. The read side was fine — the task correctly filtered on the data interval. The **write** side was an `INSERT ... SELECT` into the mart. The first (buggy) run had already written that week's rows; the `completed` reprocess ran the fixed query and *appended a second copy*. `completed` doesn't delete the old run's output — it just makes a new run, and if your write appends, you now have two.

- **Make writes overwrite the interval, not append to it.** `MERGE` on the interval key, or `DELETE WHERE interval = X` then insert, or write to a partition you replace wholesale. Then a reprocess is a true redo, not a duplicate.
- **`completed` is a hammer; reach for `failed` first.** If only Tuesday broke, `--reprocess-behavior failed` touches only Tuesday. Recompute the whole range only when the *logic* changed, not when one run flaked.
- **Rehearse with `--dry-run`.** It lists exactly which intervals the backfill would create before it creates them — cheap insurance against backfilling a wider range than you meant.

---

## 9. Verify + commit

```bash
python dags/s10/s10_assignment.py
airflow dags test s10_assignment 2026-01-03
airflow backfill create --dag-id s10_assignment --from-date 2026-01-01 --to-date 2026-01-05
python -m pytest tests/ -v
git add -A && git commit -m "session 10: backfill" && git push
```

Done when a `completed` reprocess of the same range produces the identical output it did the first time. Tick Session 10 in `docs/course/README.md`.

**Pre-push habit:** `ruff check dags/ include/ tests/ --select E,F,AIR3 && python -m pytest tests/ -v` before every push — green locally means green CI.

Sources:
[Backfill — Airflow 3.3](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/backfill.html),
[DAG runs & catchup — Airflow 3.3](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dag-run.html),
[CLI reference: backfill create — Airflow 3.3](https://airflow.apache.org/docs/apache-airflow/stable/cli-and-env-variables-ref.html)
