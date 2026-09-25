# Practical P4 · Backfilling the medallion

**Spec only — you design and write everything.** Builds directly on **[Practical P3](P3-bq-stored-proc-medallion.md)** (your raw/bronze/gold stored procedures) and **[Session 07 · Backfill](../stage-2-scheduling/07-backfill.md)** (idempotency, `airflow backfill create`, `--reprocess-behavior`). No code here, on purpose.

**Goal:** P3's procedures rebuild the **whole table** every run — that's fine for "the latest state" but it means you can't reprocess just one bad day without recomputing everything, and it isn't something `airflow backfill create` can meaningfully drive (there's no per-date output to create one run per interval against). Turn each layer **date-aware**: one call processes **one date's slice** and overwrites only that slice, leaving every other date untouched. Then use Airflow's native backfill to fill a week of history and safely reprocess it after a logic change — proving the exact idempotency rule Session 07 teaches, on real infrastructure you already built.

---

## The scenario

StackPulse's `gold.daily_health` mart works, but it's a full rebuild every time — expensive at scale, and impossible to "just redo last Tuesday." You're asked to make it **partition-safe**: each run owns exactly one date, in and out. Once that's true, backfilling history — or fixing a bug and reprocessing only the affected week — becomes Airflow's job, not a manual BigQuery chore.

---

## What you build

### Part A — parametrize the three procedures (in BigQuery)

Modify `raw_load`, `questions_load` (bronze), and `questions_load` (gold) from P3 so each **accepts a `run_date` (DATE) parameter** and, for that call, touches **only that date's data**:

- **raw:** pull source rows for `run_date` only; the write must replace **that date's** rows in `raw.questions` without disturbing any other date already there.
- **bronze:** recompute `bronze.questions_clean` for `run_date` only, same rule — other dates' rows survive untouched.
- **gold:** recompute the **single row** in `gold.daily_health` for `run_date`, leaving every other day's row exactly as it was.

**The hard constraint, stated plainly:** calling any procedure with `run_date = '2026-01-03'` must never change what's stored for `2026-01-02` or `2026-01-04`. If it does, it's not partition-safe — it's a full rebuild wearing a costume.

### Part B — wire the DAG to the run's own date, not the wall clock

Update your P3 DAG (or a copy of it) so each `CALL` passes the run's own date — the exact value Session 07 §5 calls out (`{{ ds }}` / `data_interval_start`), **never** `datetime.now()`. This is what makes a backfilled run for January 3rd actually process January 3rd, no matter what day you run the backfill on.

### Part C — backfill it

1. `airflow backfill create` a week of history with `--reprocess-behavior none`. Confirm: seven runs, seven distinct `gold.daily_health` rows, one per date — and a date **outside** the range is provably unchanged (check it before and after).
2. Re-issue the exact same command. Confirm **nothing new** is created (the intervals already have runs) — Session 07 §4's `none` behavior.
3. Now simulate the "found a bug in the transform" scenario from Session 07 §8: change something real in the bronze cleaning logic, then re-run the same week with `--reprocess-behavior completed`. Confirm all seven dates recompute — and confirm `gold.daily_health` still has **exactly one row per date**, not two. That's the specific bug Session 07's production tip warns about (a `completed` reprocess that *appends* instead of *overwrites* doubles the numbers) — your job is to prove your pipeline doesn't have it.

---

## Constraints

- Reuse P3's datasets and structure (`raw`, `bronze`, `gold`) — modify the procedures in place, don't spin up parallel tables.
- Every procedure call for a given `run_date` is **idempotent**: calling it twice for the same date leaves that date's data identical, and never touches any other date.
- The DAG derives `run_date` from the run's **own data interval**, never the wall clock — this is non-negotiable per Session 07 §5.
- Still cost-safe: cap bytes on every job. Note honestly in your own write-up if your overwrite technique requires scanning the whole table to rebuild it minus/plus one date — that's a real cost tradeoff worth naming, not hiding.
- Passes the repo's integrity gates (owner / retries ≥ 1 / tags) and `pytest`.

---

## Acceptance criteria

- All three procedures accept `run_date` and are individually idempotent **per date**.
- Backfilling 7 days with `--reprocess-behavior none` produces 7 independent `gold.daily_health` rows; a date outside the range is unchanged.
- Re-running the identical backfill command creates nothing new.
- After a bronze logic change, `--reprocess-behavior completed` over the same 7 days recomputes all of them — and `gold.daily_health` still has **exactly one row per date** (no duplication).
- `python -m pytest tests/ -v` stays green.

---

## Pointers (names only — look these up, don't copy code)

- A DDL-only way to "overwrite just one date" without touching the rest: rebuild the table as *everything except that date, unioned with the freshly computed date* — same `CREATE OR REPLACE TABLE ... AS SELECT` shape you already used in P3 to dodge the Sandbox DML block, just with a `WHERE date != run_date` kept-half added.
- The Jinja/context value that gives a task **its own run's date**, never today's date (Session 06 & 07 territory).
- `airflow backfill create`'s three `--reprocess-behavior` values, and which one you reach for in step 3 above.
- (Forward-looking, not required) real BigQuery **partitioned tables** (`PARTITION BY`) are how production does per-date overwrites without rescanning the whole table — worth knowing exists, not required to implement here.

Done when a week backfills cleanly, a `completed` reprocess doesn't double anything, and an untouched date stays untouched. Tick P4 in `docs/course/README.md`.
