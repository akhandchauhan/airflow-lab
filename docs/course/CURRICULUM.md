# The Curriculum — Airflow the way a company actually adopts it

You're the **founding data engineer at StackPulse**, a startup that sells
community-health analytics on top of the public Stack Overflow dataset
(`bigquery-public-data.stackoverflow`). You'll build the data platform the way a real
company does — **each stage exists because the business hit a need**, not because a
syllabus said so.

The order is deliberate and matches how teams actually grow into Airflow:

```
prove it works → build basic DAGs → put them on a clock → react to data
     → connect the warehouse & add brains → trust it → scale & harden → capstone
```

> **How to use this:** this file is the **path** (the why + the order). The
> **[README](README.md)** is the **tracker** (scoreboard + what's done). Each session
> has a note in `docs/course/` and two starter files in `dags/s<N>/`
> (`s<N>_examples.py` + `s<N>_assignment.py`). Do a session, tick it in the README.

Legend: ✅ done · 🟡 written, not finished · ⬜ to be written.

---

## Stage 0 · Day one — "does this thing even work?"

**The need:** before betting the company's pipelines on a tool, prove it runs and you
can see what it's doing.

| Topic | What you learn | Status |
|---|---|---|
| **00 · First DAG & testing** → [note](00-first-dag-testing.md) | DAG/task/run model, the smallest DAG, and proving it works 3 ways: parse, `airflow dags test`, integrity gates | ✅ |

**Why first:** you can't build on a black box. You learn to *run, watch, and test* a
DAG before you write a real one.

---

## Stage 1 · Ship the first pipelines (build basic DAGs)

**The need:** get data moving with pipelines that are readable and don't fall over.

| Session | Topic | Why the company needs it | Status |
|---|---|---|---|
| 01 | **TaskFlow** — `@dag`/`@task`, XCom | the basic unit: tasks that pass data | ✅ |
| 02 | **Operators & dependencies** — `>>`, `chain` | wire steps into an order | ✅ |
| 03 | **TaskGroups** | keep a growing DAG readable | ✅ |
| 05 | **Retries** | a flaky network call shouldn't page you | ✅ |
| 07 | **Params** | run the same DAG with different inputs | ✅ |
| recap | **Jinja templating** — `{{ ds }}` / `{{ params.x }}` in operator fields | templated SQL/bash without Python → [recap §6](stage1-recap.md) | ✅ |

📄 **All of Stage 1 in one file:** [stage1-recap.md](stage1-recap.md) — TaskFlow,
operators, TaskGroups, retries, params, with copy-runnable code.

**Milestone:** StackPulse can build, wire, and re-run a basic pipeline by hand.

---

## Stage 2 · Put it on the clock (time-based / cron)

**The need:** nobody should click "run" every morning — pipelines must run themselves,
on time, and be safe to re-run.

| Session | Topic | Why | Status |
|---|---|---|---|
| 09 | **Schedules & intervals** — cron, `data_interval`, `catchup` | run daily/hourly; process the right window | 🟡 |
| 10 | **Backfill** | fill history on purpose without melting the scheduler | ⬜ |
| 11 | **Timetables** | cadences cron can't express (business days, custom) | ⬜ |

**Milestone:** the daily community-health report runs itself and is idempotent per
window.

---

## Stage 3 · React to data, not the clock (asset-based)

**The need:** a timer only *hopes* the upstream data landed. The company wants runs
that fire **when the data is actually ready**.

| Session | Topic | Why | Status |
|---|---|---|---|
| 12 | **Assets & data-aware scheduling** (end-to-end) → [note](12-assets.md) | run the report the moment the table updates | 🟡 |
| 13 | **Asset logic** — `AssetAll` / `AssetAny`, aliases | run only when *several* inputs are ready | ⬜ |
| 14 | **Event-driven** — watchers, message queues | trigger from outside events, not just Airflow | ⬜ |

**Milestone:** the pipeline is a graph of data dependencies, not a wall of cron lines.

---

## Stage 4 · Connect the warehouse & add brains (the ELT era)

**The need:** real data in BigQuery, pipelines that make decisions and scale over many
inputs. (ELT is ~90% of what companies use Airflow for.)

| Session | Topic | Why | Status |
|---|---|---|---|
| 15 | **Connections & Hooks** | talk to BigQuery/DBs with creds kept out of code | 🟡 |
| 06 | **BigQuery ground-up → advanced** | the warehouse: cost, partitioning, idempotent loads | ✅ |
| 04 | **Branching & trigger rules** | pipelines that choose a path and survive skips | ✅ |
| 08 | **Dynamic task mapping** | fan out over N files/regions decided at run time | 🟡 |

**Milestone:** StackPulse runs real cost-capped BigQuery ELT that branches and scales.

---

## Stage 5 · Trust it, and get paged when it breaks

**The need:** bad data must not reach customers, and a failure must reach a human.

| Session | Topic | Why | Status |
|---|---|---|---|
| 16 | **Data quality** — SQL checks, circuit breaker | stop a bad number before the VP sees it | ⬜ |
| 17 | **Alerting & callbacks** — `on_failure_callback`, Slack | know within minutes, not days | ⬜ |
| 18 | **Observability** — logging, metrics, lineage | see *why* it broke | ⬜ |

**Milestone:** a broken run self-checks, halts, and alerts on-call.

---

## Stage 6 · Scale & harden for production

**The need:** many DAGs, many teams, cost and reliability under control.

| Session | Topic | Why | Status |
|---|---|---|---|
| 19 | **Sensors & deferrable operators** | wait for things without burning workers | ⬜ |
| 20 | **Variables & Secrets backends** | config and secrets done right at scale | ⬜ |
| 21 | **Executors & concurrency** — pools, priority | keep 100 DAGs from trampling each other | ⬜ |
| 22 | **DAG versioning & CI/CD** | ship safely; every run pinned to its code | ⬜ |
| 23 | **dbt via Cosmos, push-down ELT** | integrate the modern transform layer | ⬜ |

**Milestone:** the platform survives real load, real teams, and real deploys.

---

## Capstone · The StackPulse platform

Wire every layer into **one** pipeline on `bigquery-public-data.stackoverflow`:
asset-triggered, cost-controlled BigQuery ELT, branching on data, dynamic-mapped over
sources, quality-gated, Slack-alerting, tested, and CI-gated. The thing a real data
team would actually run.

---

## Where you are right now

Done: **01, 02, 03, P1, 04, 05, 06, 07**. In progress: **08, 09**. The fastest path
that respects the company story from here: finish **Stage 2** (09 schedules → 10
backfill → 11 timetables), then **Stage 3 assets** — the part that turns a pile of
cron jobs into a real data platform.

Sources:
[Astronomer — Airflow 101 learning path](https://academy.astronomer.io/path/airflow-101),
[Airflow ETL/ELT use cases](https://airflow.apache.org/use-cases/etl_analytics/),
[Airflow production best practices — idempotency, quality, assets](https://www.astronomer.io/airflow/use-cases/)
