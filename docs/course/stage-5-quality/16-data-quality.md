# Session 16 · Data Quality — the circuit breaker

**Goal:** a green pipeline is not the same as a correct number, and this session closes that gap. The failure mode you are defending against is *success with garbage* — a load that ran, succeeded, and quietly published zeros or nulls to a customer dashboard. The one idea to get right: a data-quality check is a **circuit breaker** you wire *between* the compute step and the publish step, so a bad number **trips the breaker and stops the run** instead of flowing downstream. You will learn the two shapes that breaker comes in — a task that **fails** the run (`BigQueryCheckOperator`, `BigQueryValueCheckOperator`, `SQLColumnCheckOperator`, `SQLTableCheckOperator`) and a task that **skips** the rest quietly (`@task.short_circuit`) — and, more importantly, *when* each is the right reaction. Back on the Stack Overflow spine, cost-capped, real BigQuery.

---

## 1. Why a quality gate has to exist (the silent-corruption problem)

Everything you have built so far protects against the pipeline *breaking*: retries (Session 04) handle a task that throws, assets (Session 09) handle a report that runs too early. None of them protect against the pipeline *lying*. Consider the exact shape of the bug: the upstream loader hit an empty source, wrote **zero rows**, and returned exit code 0. Airflow saw success. The downstream mart read zero rows, computed an answer rate of `0%`, and published it. Every task is green. The dashboard is wrong. Nobody is paged, because from Airflow's point of view **nothing failed** — and that silence is the whole problem.

The running analogy for this session is the **electrical circuit breaker** in your house. Current (your data) flows from the panel to the appliances (the dashboards, the exec report, the downstream marts). A surge or a short — a table that came back empty, a null rate that spiked, a count that halved overnight — is a *bad current*. Without a breaker, that bad current reaches the appliances and fries them: the exec opens a dashboard of zeros. A breaker sits *in the line* and **trips before the surge reaches anything**, cutting the circuit. A data-quality check is exactly that: a task placed in the dependency line whose only job is to trip — fail or skip — the instant a number looks wrong, so the bad value never reaches a human.

The rule that makes the rest of the session make sense: **the check is not measuring your code, it is measuring your data.** Your SQL can be perfect and the check still trips, because the *source* was bad. That is the point — you want to be woken by a loud failure at 2am, not by an angry customer at 9am.

---

## 2. Two reactions: fail loud vs skip quiet

A check can react two ways when the data is bad, and choosing wrong is the most common mistake here.

| Reaction | Task ends as | Downstream | Pages you? | Reach for it when… |
| --- | --- | --- | --- | --- |
| **Fail** (check operators) | `failed` (raises `AirflowException`) | skipped (unless a trigger rule overrides) | **yes** — via retries/alerts | bad data is an **incident** — the number is wrong and someone must look |
| **Skip** (`@task.short_circuit`) | `success`, downstream `skipped` | skipped | **no** | bad data is a **non-event** — "nothing to publish today," and that is fine |

The distinction is *is this a problem I need to know about, or a normal empty day?* An empty Monday load for a batch that genuinely sometimes has no rows is a short-circuit — skip the publish, no alert, no red run. A source that should *never* be empty coming back empty is a **failure** — trip the breaker loud so retries fire and Session 17's alert goes out. Same mechanical effect (downstream does not run); opposite operational meaning.

---

## 3. The declarative gate — `SQLColumnCheckOperator` / `SQLTableCheckOperator`

These live in the provider that every SQL provider builds on:

```python
from airflow.providers.common.sql.operators.sql import (
    SQLColumnCheckOperator,       # ← per-column rules
    SQLTableCheckOperator,        # ← whole-table rules
)
```

> **What is `apache-airflow-providers-common-sql`?** It is the shared base package every database provider (Postgres, MySQL, BigQuery, Snowflake…) depends on. These two operators are **portable** — the *same* check definition runs against any of them, because they only need a `conn_id` whose hook speaks the DB-API. That is why they take a `conn_id`, a `table`, and a dict of rules, and nothing vendor-specific.

**`SQLColumnCheckOperator`** — you describe each column and the rules it must satisfy in `column_mapping`. Airflow generates one `CASE`-based SQL statement, runs it, and fails the task if any rule is violated:

```python
SQLColumnCheckOperator(                                  # ← THE MECHANIC: rules → generated SQL → pass/fail
    task_id="check_columns",
    conn_id="google_cloud_default",
    table="bigquery-public-data.stackoverflow.posts_questions",
    column_mapping={
        "id": {
            "null_check": {"equal_to": 0},               # zero nulls allowed
            "unique_check": {"equal_to": 0},             # zero duplicates
        },
        "answer_count": {
            "min": {"geq_to": 0},                        # no negative answer counts
        },
    },
    partition_clause="creation_date >= '2022-01-01'",    # scope the scan (and the cost)
)
```

- Check types: `null_check`, `distinct_check`, `unique_check`, `min`, `max`.
- Conditions inside each: `equal_to`, `greater_than`, `geq_to`, `less_than`, `leq_to`, plus `tolerance` (a fractional wiggle room) and a per-check `partition_clause`.
- `accept_none=True` (default) treats SQL `NULL` results as passing — set it `False` when a null result should itself be a failure.

**`SQLTableCheckOperator`** — for rules that span columns or the whole table, you write the boolean SQL yourself in `check_statement`:

```python
SQLTableCheckOperator(
    task_id="check_table",
    conn_id="google_cloud_default",
    table="bigquery-public-data.stackoverflow.posts_questions",
    checks={
        "row_count_check": {"check_statement": "COUNT(*) > 0"},                # not empty
        "answered_le_total": {"check_statement": "COUNTIF(answer_count > 0) <= COUNT(*)"},
    },
    partition_clause="creation_date >= '2022-01-01'",
)
```

> **BigQuery has native subclasses.** `BigQueryColumnCheckOperator` and `BigQueryTableCheckOperator` (from `airflow.providers.google.cloud.operators.bigquery`) are the same operators wired straight to `BigQueryHook` — use those when you want BQ-native behaviour; the `common.sql` ones above work against BigQuery too via `google_cloud_default`.

---

## 4. The value gate — `BigQueryCheckOperator` / `BigQueryValueCheckOperator`

When you want to gate on a **single computed number** rather than declare column rules, the BQ-native pair is the tool. Both come from one import:

```python
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCheckOperator,        # first row must be all-truthy
    BigQueryValueCheckOperator,   # first value must equal pass_value
)
```

**`BigQueryCheckOperator`** runs a one-row query and applies Python `bool()` to every value in that row; **any falsy value fails the task** (`0`, `None`, `""`, `False` all trip it — see Session 13 §9 for the full truth table). It is the natural "is this non-zero?" gate:

```python
BigQueryCheckOperator(                                   # ← THE MECHANIC: any falsy value in row 1 → raise → fail
    task_id="assert_has_rows",
    gcp_conn_id="google_cloud_default",
    use_legacy_sql=False,
    location="US",
    sql="SELECT COUNT(*) FROM `bigquery-public-data.stackoverflow.posts_questions` WHERE answer_count = 0",
)
```

**`BigQueryValueCheckOperator`** asserts the query's value **equals a `pass_value`**, optionally within a `tolerance` (a fraction, so `0.05` = ±5%). Use it when "correct" means "close to an expected number," not merely "non-zero":

```python
BigQueryValueCheckOperator(
    task_id="assert_answer_rate",
    gcp_conn_id="google_cloud_default",
    use_legacy_sql=False,
    location="US",
    sql="SELECT ROUND(100*COUNTIF(answer_count>0)/COUNT(*),1) FROM `bigquery-public-data.stackoverflow.posts_questions`",
    pass_value=72.0,
    tolerance=0.05,                                       # accept 68.4–75.6 (±5%)
)
```

Cousins worth knowing (Session 13 listed them): `BigQueryIntervalCheckOperator` compares a metric today vs `days_back` ago (drift detection), and the column/table subclasses from §3.

---

## 5. The soft brake — `@task.short_circuit`

The check operators are a *hard* breaker: they raise, the task goes red, retries and alerts fire. Sometimes you want the opposite — stop the pipeline **without** raising an alarm, because "no data today" is a legitimate outcome. That is `@task.short_circuit`:

```python
from airflow.sdk import task

@task.short_circuit                                      # ← THE MECHANIC: falsy return → skip everything downstream
def gate_on_rows(row_count: int) -> bool:
    return row_count > 0        # False → the rest of the DAG is skipped, but THIS task is a green success
```

The mechanic: the decorated callable returns a value; if it is **falsy**, every downstream task is marked `skipped` and the short-circuit task itself finishes **green**. If truthy, the DAG proceeds normally. No exception, no red, no page.

- `ignore_downstream_trigger_rules` defaults to **`True`** — a "hard short" that blindly skips **all** downstream tasks regardless of their trigger rules. Set it to `False` (`@task.short_circuit(ignore_downstream_trigger_rules=False)`) for a "soft short" that skips only the *immediate* downstream and lets the scheduler evaluate everyone else's trigger rules normally.
- It is the TaskFlow face of the classic `ShortCircuitOperator`; same behaviour, decorator ergonomics.

The choice between §4 and §5 is the choice from §2: `BigQueryCheckOperator` when an empty result is an **incident**, `@task.short_circuit` when it is a **normal quiet day**.

---

## 6. Where the gate goes (fail closed)

A breaker is useless downstream of the appliance it protects. Placement is the whole game:

```
load  ──▶  🔌 CHECK  ──▶  publish
          (trips here)     (never runs on bad data)
```

- Put the check **after** the data is produced and **before** anything a human or another team consumes it. The check must sit *between* compute and publish, never after publish.
- **Fail closed, not open.** If the check itself errors, the run should end red and the publish should not happen — which is the default (a raised exception stops the branch). Never wrap a check in a `try/except` that swallows the error; that turns your breaker into a decoration.
- One dedicated gate task beats sprinkling asserts inside the compute task: the gate shows up as its own red node in the grid, so at 2am you see *"the check failed,"* not *"something in the 200-line load task failed."*

---

## 7. Complete runnable reference — a quality gate on the Product Health pipeline

One DAG: compute a metric into a scalar, run a **hard** BigQuery check that the table is non-empty, run a **value** check that the answer rate is in a sane band, and a **short-circuit** that quietly stops the publish when there is nothing to report. Everything read-only on the Stack Overflow spine, every query capped, no `SELECT *`.

```python
# dags/stage-5-quality/s16/s16_examples.py
from __future__ import annotations

import pendulum
from airflow.sdk import dag, task
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCheckOperator,
    BigQueryValueCheckOperator,
)

CONN = "google_cloud_default"
QUESTIONS = "bigquery-public-data.stackoverflow.posts_questions"
CAP = "2000000000"          # 2 GB max bytes billed — reject anything bigger


@dag(
    dag_id="s16_examples",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["session-16", "data-quality", "stackoverflow"],
    default_args={"owner": "akhand", "retries": 1},
)
def pipeline():

    # HARD gate: fail the run (incident) if the table has zero rows in scope
    assert_has_rows = BigQueryCheckOperator(
        task_id="assert_has_rows",
        gcp_conn_id=CONN,
        use_legacy_sql=False,
        location="US",
        sql=f"SELECT COUNT(*) FROM `{QUESTIONS}` WHERE creation_date >= '2022-01-01'",
    )

    # VALUE gate: fail if the answer rate drifts outside an expected band
    assert_answer_rate = BigQueryValueCheckOperator(
        task_id="assert_answer_rate",
        gcp_conn_id=CONN,
        use_legacy_sql=False,
        location="US",
        sql=(
            f"SELECT ROUND(100*COUNTIF(answer_count>0)/COUNT(*),1) "
            f"FROM `{QUESTIONS}` WHERE creation_date >= '2022-01-01'"
        ),
        pass_value=72.0,
        tolerance=0.1,          # accept ±10% around 72
    )

    # SOFT brake: read a small scalar, and skip publish (green) if nothing to report
    @task
    def measure_backlog() -> int:
        hook = BigQueryHook(gcp_conn_id=CONN, use_legacy_sql=False, location="US")
        sql = (
            f"SELECT COUNT(*) FROM `{QUESTIONS}` "
            f"WHERE answer_count = 0 AND creation_date >= '2022-01-01'"
        )
        n = int(hook.get_first(sql)[0])
        print(f"unanswered backlog = {n:,}")
        return n

    @task.short_circuit
    def gate_on_backlog(backlog: int) -> bool:
        return backlog > 0      # falsy → publish is skipped quietly, no alert

    @task
    def publish(backlog: int) -> None:
        print(f"publishing Product Health: backlog={backlog:,}")

    backlog = measure_backlog()
    proceed = gate_on_backlog(backlog)
    [assert_has_rows, assert_answer_rate] >> proceed >> publish(backlog)


pipeline()
```

```bash
python dags/stage-5-quality/s16/s16_examples.py
airflow dags test s16_examples 2026-01-01
```

Check **BigQuery Job history**: every query is a `COUNT`/aggregate over `answer_count`/`creation_date` only — a few hundred MB, well under the 2 GB cap. Flip the value-check `pass_value` to something absurd (e.g. `10.0`) and rerun to watch `assert_answer_rate` trip the breaker and skip `publish`.

---

## 8. Build spec — your challenge (no solution)

**File:** `dags/stage-5-quality/s16/s16_assignment.py` · **dag_id:** `s16_assignment`

Build a **quality gate** that guards a Product Health metric before it publishes.

**The problem:**

- Pick a metric on `posts_questions` (or `posts_answers`) — e.g. the count of questions with `view_count > 1000`, or the daily answer rate.
- Add **one declarative check** (`SQLColumnCheckOperator` *or* `SQLTableCheckOperator`, or their BigQuery subclasses) asserting a column/table rule — e.g. `id` has zero nulls, or `row_count_check` is `> 0`.
- Add **one value/threshold check** (`BigQueryCheckOperator` *or* `BigQueryValueCheckOperator`) that **fails** the run when the metric is empty or out of band.
- Add **one `@task.short_circuit`** that **skips** the publish quietly on a legitimately empty day (and decide, in a comment, why *this* case is a skip and not a failure).
- A final `@task` "publishes" (logs) the metric only when all gates pass.

**Constraints:**

- Every query carries `maximumBytesBilled`; no `SELECT *`; `"useLegacySql": False`.
- The gate tasks sit **between** compute and publish (§6) — publish must be unreachable on bad data.
- Names clearly distinct (R12): `task_id` a noun, the function a verb form, the variable its role.
- Passes the integrity gates: `tags`, real `owner`, `retries >= 1`.

**Acceptance criteria:**

- `python dags/stage-5-quality/s16/s16_assignment.py` parses (prints nothing).
- `airflow dags test s16_assignment 2026-01-01` runs green on real data.
- Temporarily tightening the threshold makes the value check go **red** and `publish` **skip** — proving the breaker trips.
- **Job history** shows every query within the cap.
- `python -m pytest tests/ -v` stays green.

**One nudge (only if stuck):** the difference between "fail" and "skip" is the difference between a check operator (raises) and `@task.short_circuit` (returns falsy) — you do not need any custom exception handling for either.

---

## 9. Production tip — the check that lies is worse than no check

The bug that pages you at 2am — except it *doesn't* page you, and that is the incident. A well-meaning engineer wrapped the quality check in `try/except: pass` because it "kept failing during the migration and blocking deploys." The migration ended; the `try/except` stayed. Six weeks later the source went empty, the check *would* have caught it, but the swallowed exception let the run go green and publish zeros. The breaker was there — someone had taped the switch to "on."

- **A check you can bypass is not a check.** Never `try/except` a quality gate to keep a run green; if a check is too noisy, fix the threshold or the `tolerance`, don't muzzle it. The whole value of the gate is that it *can* stop the pipeline.
- **Fail closed.** When the check itself can't run, the safe default is a red run and no publish — never "couldn't check, so assume it's fine."
- **Say why an empty day is OK — in code.** If you use `@task.short_circuit` for an empty load, leave a one-line comment on *why* empty is legitimate here. The next engineer needs to know it is a skip by design, not a bug someone silenced.
- **Checks are cheap, wrong numbers are not.** A `COUNT(*)` gate scans metadata-sized bytes; an exec acting on a zeroed dashboard costs a meeting and your credibility. Put the gate in.

---

Sources:
[Common SQL check operators — Airflow](https://airflow.apache.org/docs/apache-airflow-providers-common-sql/stable/operators.html),
[BigQuery operators (Check / ValueCheck / Column / Table) — Airflow](https://airflow.apache.org/docs/apache-airflow-providers-google/stable/operators/cloud/bigquery.html),
[ShortCircuitOperator & @task.short_circuit — Airflow standard provider](https://airflow.apache.org/docs/apache-airflow-providers-standard/stable/operators/python.html),
[Data quality checks — Astronomer](https://www.astronomer.io/docs/learn/data-quality/)
