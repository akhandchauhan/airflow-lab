# Capstone · The StackPulse Platform

**Goal:** wire every layer the course taught into **one** production-shaped pipeline on `bigquery-public-data.stackoverflow` — an asset-triggered, cost-capped BigQuery ELT that branches on the state of the data, dynamic-maps its per-tag rollups, gates on data quality before it publishes, and pages you on Slack when it breaks — all of it tested and CI-green. This is not a new topic; it is the assembly. Each layer below names the session it came from, so the capstone doubles as a map of the whole course. You are the founding data engineer at StackPulse, and this is the pipeline the company actually runs every day to sell community-health analytics.

---

## 1. The product: what "Product Health" means

StackPulse sells a daily read on the health of the Stack Overflow community. One pipeline run produces one day's **Product Health mart** — a small table other teams and dashboards read. The metrics that matter, all derived from the spine tables (R24):

| Metric | Source | Meaning |
| --- | --- | --- |
| **answer rate** | `posts_questions` (`answer_count`) | share of questions that got at least one answer — the headline number |
| **unanswered backlog** | `posts_questions` where `answer_count = 0` | how many questions are still going begging |
| **new questions / day** | `posts_questions.creation_date` | inbound volume for the day being processed |
| **per-tag health** | `posts_questions.tags` (pipe-delimited) + `tags` | answer rate broken out for each of the top-N tags — the dynamic-mapped layer |
| **active askers** | `users`, `posts_questions.owner_user_id` | rough engagement signal (optional stretch) |

The pipeline's job is to compute these cheaply and safely, land them in a mart, and refuse to publish a bad number.

---

## 2. Architecture — which session each layer came from

The whole point of the capstone is that you have already built every one of these in isolation. Now they compose into a single DAG (plus its producer). Read this table as the parts list; §5 is the build spec that assembles them.

| Layer | What it does here | Session it came from |
| --- | --- | --- |
| **Trigger — assets** | the mart DAG runs when the raw-load DAG signals "today's data landed", not on a hopeful clock | Session 09 · Assets, Session 10 · Asset logic (`AssetAll`) |
| **Schedule floor** | a time floor so a dead producer can't mean a dead report (`AssetOrTimeSchedule`) | Session 06 · Schedules |
| **Params** | the run takes a target date (and top-N for tags) as validated input, so the same DAG backfills any day | Session 05 · Params |
| **Connections & hooks** | every BigQuery touch resolves `google_cloud_default` at run time; no secret in the file | Session 12 · Hooks |
| **Cost-capped ELT** | the transforms run *in* BigQuery via `BigQueryInsertJobOperator`, every query carrying `maximumBytesBilled`; Airflow only orchestrates and reads small scalars | Session 13 · BigQuery, P1 |
| **Branch on data** | after a freshness probe, choose a path — publish vs skip-and-alert on a stale/empty day | Session 14 · Branching & trigger rules |
| **Dynamic mapping** | fan the per-tag rollup out over the top-N tags decided at run time, then fan the results back in | Session 15 · Dynamic task mapping |
| **Quality gate** | a `BigQueryCheckOperator` (and a value/interval check) fails the run *before* the mart publishes if a metric is empty, null, or drifts implausibly | Session 13 §9, Session 16 · Data quality |
| **Slack alerting** | `on_failure_callback` fires a Slack message the moment any task fails, so on-call knows in minutes | Session 17 · Alerting |
| **Idempotency** | the load overwrites exactly the target date's partition (`WRITE_TRUNCATE` on a partition / `MERGE`) so any retry or backfill lands the same rows | Session 13 §4 |
| **Tested + CI-gated** | passes the integrity gates (real `owner`, `retries >= 1`, non-empty `tags`) and `pytest` stays green | R15, Session 22 · CI/CD |

The dependency shape, end to end:

```
raw-load DAG (producer)
  └─ load_raw  ──outlets=[questions_raw & answers_raw]──▶  asset event
                                                              │
Product Health mart DAG (consumer)   schedule=(questions_raw & answers_raw) with a daily time floor
  probe_freshness ─▶ @task.branch ─┬─▶ publish path ─▶ build_daily_metrics (capped ELT, idempotent)
                                   │                     └─▶ per_tag_health.expand(tag=top_tags())  (mapped)
                                   │                            └─▶ reduce_tag_health
                                   │                                 └─▶ quality_gate (BigQueryCheckOperator)
                                   │                                      └─▶ publish_mart  (WRITE_TRUNCATE partition)
                                   └─▶ skip_and_note (stale/empty day)
  everything ─▶ on_failure_callback = send_slack_notification(...)
```

---

## 3. Data contract — the tables and the traps

- **Spine tables (R24):** `posts_questions`, `posts_answers`, `users`, `tags`, `votes` under `bigquery-public-data.stackoverflow`, all in multi-region **US** — set `location="US"` on every job and hook.
- **Workhorse columns:** `creation_date` (the date you filter and partition on) and `answer_count` (0 ⇒ unanswered). Never `SELECT *`; name only these plus what a metric needs.
- **`tags` is pipe-delimited** on `posts_questions` (e.g. `python|pandas|airflow`) — splitting it (`SPLIT`, `UNNEST`) is how per-tag health is computed; the standalone `tags` dimension table is tiny and safe to read whole.
- **The date trap:** the public tables are historical (they do not grow to *today*). Your Param default must be a date that exists in the data (2022 range), not `{{ ds }}` blindly — otherwise a "fresh" run legitimately finds zero rows and the branch skips. State your chosen anchor date in the DAG.

---

## 4. Cost safety — non-negotiable (R16/R17)

Every query in the capstone obeys the same rules P1 and Session 13 drilled, because one uncapped scan is a budget incident, not a style nit:

- **`maximumBytesBilled` on every `BigQueryInsertJobOperator` query** — a hard cap that rejects the query before billing. Size it to the metric (a filtered daily aggregate is small; pick a cap like `"2000000000"` / 2 GB and tighten from Job history).
- **`"useLegacySql": False`** everywhere (legacy SQL is being restricted after 2026-06-01).
- **Prefer `COUNT`/aggregates** (0 bytes for `COUNT(*)`), filter on `creation_date` so you scan one day not all history, and **never `SELECT *`**.
- **Orchestrate, don't compute (R20):** the operator's XCom is the job id; pull only small scalars back through `BigQueryHook.get_first` in a `@task`.
- **Pin the provider** (`apache-airflow-providers-google`, `apache-airflow-providers-slack`); never Cloud Composer; never commit the key (R18/R19 — creds come from the `google_cloud_default` Connection).

---

## 5. Build spec — the capstone (no solution)

**Files:** `dags/capstone/capstone_assignment.py` (build) · `dags/capstone/capstone_examples.py` (prototype each layer here first) · **dag_ids:** `capstone_assignment` (the mart consumer) plus one producer dag_id of your choosing (e.g. `capstone_raw_load`).

Assemble the full Product Health pipeline. This is a problem statement — you design the *how*; the sessions above are your reference for each mechanic.

### 5.1 Problem statement

Build a **two-DAG** system on the Stack Overflow spine that produces one day's Product Health mart, driven by data availability rather than a clock, that makes a decision based on the data, scales its per-tag work over a run-time list, refuses to publish a bad number, and pages Slack on any failure.

**Producer DAG** (the raw-load signal):

- One (or two) tasks that stand in for "today's raw questions/answers landed" — for the capstone they may be capped read-only probes on `posts_questions` / `posts_answers` for the target date — that **`outlets`** the raw asset(s). Success writes the asset event (Session 09).

**Consumer DAG — `capstone_assignment`** (the mart):

1. **Scheduled on the asset(s)** with `schedule=(questions_raw & answers_raw)` (`AssetAll`, Session 10), wrapped so there is also a **daily time floor** (`AssetOrTimeSchedule`, Session 06) — the report still runs once a day even if a producer dies.
2. **Params (Session 05):** `target_date` (string, defaults to your in-data anchor) and `top_n` (integer, `minimum=1`, default e.g. 10). Read them via `get_current_context()["params"]`. Bad input is rejected at trigger.
3. **Freshness probe + branch (Session 14):** a task reads a small scalar (row count for `target_date`) through a **hook** (Session 12); a `@task.branch` returns the **task_id** of either the publish path or a `skip_and_note` path when the day is empty/stale.
4. **Capped ELT (Sessions 06 / P1):** `BigQueryInsertJobOperator` jobs compute the daily metrics into a staging area, every query capped and `creation_date`-filtered, **idempotent** via `WRITE_TRUNCATE` on the `target_date` partition (or `MERGE`).
5. **Dynamic-mapped per-tag health (Session 15):** a task returns the **top-N tags** (from the tiny `tags` table or a capped query) at run time; a mapped task computes each tag's answer rate with `.partial()` freezing the constants and `.expand()` over the tag list; a **reduce** task fans the per-tag results back into one structure.
6. **Quality gate (Sessions 06 §9 / 16):** a `BigQueryCheckOperator` fails the run if the mart's headline metric is zero/empty; add a `BigQueryValueCheckOperator` or `BigQueryIntervalCheckOperator` for a bound/drift check. The gate sits **before** `publish_mart`, so a bad number never reaches the mart.
7. **Publish (idempotent):** the final job writes the day's row(s) into the Product Health mart table with `WRITE_TRUNCATE` on the partition.
8. **Slack alerting (Session 17):** set `on_failure_callback=[send_slack_notification(...)]` (from `airflow.providers.slack.notifications.slack`) at the DAG or task level so any failure pages on-call. Requires a `slack_default` Connection (Session 12).

### 5.2 Constraints

- Airflow 3 only, public API `airflow.sdk`; TaskFlow by default, classic operators only for BigQuery (R13).
- Every BigQuery query carries `maximumBytesBilled`, `"useLegacySql": False`, no `SELECT *` (R16/R17).
- All credentials come from Connections resolved at run time; **no secret grep-able in the files** (Session 12). Hooks instantiated **inside** tasks, never at module top level or the `@dag` body.
- The consumer references the **assets**, never the producer's dag_id (Session 09).
- Loads are **idempotent** — a rerun of any date lands identical rows (Session 13).
- Names clearly distinct per R12: `task_id` a noun, function a verb form, variable its role; `@task.branch` returns the noun task_id string; `.expand`/`.partial` called on the variable.
- Passes the integrity gates: real `owner`, `retries >= 1`, non-empty `tags` (R15).

### 5.3 Acceptance criteria

- `python dags/capstone/capstone_assignment.py` parses (prints nothing); so does the producer file.
- With the producer unpaused, its success triggers `capstone_assignment` exactly once through the asset (UI **Assets** view shows producer → asset → consumer); the daily time floor also runs it once a day with no event.
- `airflow dags test capstone_assignment 2026-01-01 --conf '{"target_date": "<in-data date>", "top_n": 5}'` runs green end to end; a `--conf` with `top_n` below `minimum` is **rejected** at trigger.
- On a date with data: the branch takes the **publish** path, the per-tag task shows as a mapped **[N]** node, the quality gate passes, and the mart partition for `target_date` is (re)written — rerunning lands identical rows.
- On an empty/stale date: the branch takes **skip_and_note**, the mart is **not** written, and no bad row publishes.
- Forcing a task to fail sends a **Slack** message via `on_failure_callback`.
- **BigQuery Job history** shows every query within its cap; `grep` finds no password/project/key literal in either file.
- `python -m pytest tests/ -v` stays green.

### 5.4 Nudges (only if stuck)

- The asset **AND** floor is one object: `AssetOrTimeSchedule(timetable=..., assets=(questions_raw & answers_raw))` — you pass the expression straight to it, you write no waiting logic (Sessions 09/13).
- The branch returns a **string** — the downstream task's `task_id` — not the function; give the skip path a trigger rule so the join task still runs (Session 14).
- The mapped list must come from an **upstream task's return**, not a literal in `.expand()` (Session 15).
- The quality gate is `BigQueryCheckOperator`: a first-row falsy value (0/NULL) **fails** the task, which stops the publish downstream (Session 13 §9).

---

## 6. War story — the capstone's 2am failure mode

The capstone earns its keep on the day the raw load *succeeds* on an empty upstream: it writes zero rows, rings the asset bell, and every naive downstream cheerfully publishes an empty dashboard to a paying customer (Session 09 §10). The capstone survives it because three habits are wired in, not bolted on. **The asset event says "done," not "good"** — so the freshness probe and the branch decide whether the day is real before any ELT runs. **The quality gate sits before the publish** — a zero headline metric fails the run at the gate, not on the customer's screen. **The failure pages a human** — `on_failure_callback` fires Slack the instant the gate raises, so on-call learns in minutes, not from an angry VP the next morning. That trio — react to data, gate before publish, alert on failure — is the difference between a pile of DAGs and a platform a company bets revenue on.

---

## 7. Verify

```bash
python dags/capstone/capstone_assignment.py
airflow dags test capstone_assignment 2026-01-01 --conf '{"target_date": "2022-08-01", "top_n": 5}'
python -m pytest tests/ -v
ruff check dags/ include/ tests/ --select E,F,AIR3
```

Done when the asset chain fires the mart, the branch and quality gate both do their job, the mapped per-tag node shows `[N]`, a forced failure hits Slack, and Job history shows only small capped scans. Then tick the **Capstone** in `docs/course/README.md`.

Sources:
[Assets & asset-scheduling — Airflow 3.3](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/assets.html),
[Dynamic Task Mapping — Airflow 3.3](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/dynamic-task-mapping.html),
[Params — Airflow 3.3](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/params.html),
[Airflow BigQuery operators](https://airflow.apache.org/docs/apache-airflow-providers-google/stable/operators/cloud/bigquery.html),
[Slack notifier — apache-airflow-providers-slack](https://airflow.apache.org/docs/apache-airflow-providers-slack/stable/notifications/slack_notifier_howto_guide.html),
[BigQuery cost best practices](https://docs.cloud.google.com/bigquery/docs/best-practices-costs)
