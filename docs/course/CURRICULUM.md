# The Curriculum — Airflow the way a company actually adopts it

You're the **founding data engineer at StackPulse**, a startup that sells community-health analytics on top of the public Stack Overflow dataset (`bigquery-public-data.stackoverflow`). You'll build the data platform the way a real company does — **each stage exists because the business hit a need**, not because a syllabus said so.

The order is deliberate and matches how teams actually grow into Airflow:

```
prove it works → build basic DAGs → put them on a clock → react to data
     → connect the warehouse & add brains → trust it → scale & harden → capstone
```

> **How to use this:** this file is the **path** (the why + the order). The **[README](README.md)** is the **tracker** (scoreboard + what's done). Notes and DAGs live in **phase folders**: `docs/course/<phase>/NN-topic.md` and `dags/<phase>/s<N>/` (`s<N>_examples.py` + `s<N>_assignment.py`). Sessions are numbered **ascending in learning order**. Do a session, tick it in the README.

Legend: ✅ done · 🟡 written & ready, not finished yet.

---

## Stage 0 · Day one — "does this thing even work?" · `stage-0-day-one/`

**The need:** before betting the company's pipelines on a tool, prove it runs and you can see what it's doing.

| # | Topic | What you learn | Status |
|---|---|---|---|
| 00 | **First DAG & testing** → [note](stage-0-day-one/00-first-dag-testing.md) | DAG/task/run model, the smallest DAG, proving it works 3 ways: parse, `airflow dags test`, integrity gates | ✅ |

---

## Stage 1 · Ship the first pipelines · `stage-1-basic-dags/`

**The need:** get data moving with pipelines that are readable and don't fall over.

| # | Topic | Why the company needs it | Status |
|---|---|---|---|
| 01 | **TaskFlow** — `@dag`/`@task`, XCom → [note](stage-1-basic-dags/01-taskflow-foundations.md) | the basic unit: tasks that pass data | ✅ |
| 02 | **Operators & dependencies** — `>>`, `chain` → [note](stage-1-basic-dags/02-classic-operators.md) | wire steps into an order | ✅ |
| 03 | **TaskGroups** → [note](stage-1-basic-dags/03-task-groups.md) | keep a growing DAG readable | ✅ |
| 04 | **Retries** → [note](stage-1-basic-dags/04-retries.md) | a flaky network call shouldn't page you | ✅ |
| 05 | **Params** → [note](stage-1-basic-dags/05-params.md) | run the same DAG with different inputs | ✅ |
| recap | **Jinja templating** — `{{ ds }}` / `{{ params.x }}` → [recap](stage-1-basic-dags/stage1-recap.md) | templated SQL/bash without Python | ✅ |

📄 **All of Stage 1 in one file:** [stage1-recap.md](stage-1-basic-dags/stage1-recap.md).

**Milestone:** StackPulse can build, wire, and re-run a basic pipeline by hand.

---

## Stage 2 · Put it on the clock (time-based / cron) · `stage-2-scheduling/`

**The need:** nobody should click "run" every morning — pipelines must run themselves, on time, and be safe to re-run.

| # | Topic | Why | Status |
|---|---|---|---|
| 06 | **Schedules & intervals** — cron, `data_interval`, `catchup` → [note](stage-2-scheduling/06-schedules.md) | run daily/hourly; process the right window | ✅ |
| 07 | **Backfill** → [note](stage-2-scheduling/07-backfill.md) | fill history on purpose without melting the scheduler | 🟡 |
| 08 | **Timetables** → [note](stage-2-scheduling/08-timetables.md) | cadences cron can't express (business days, custom) | 🟡 |

**Milestone:** the daily community-health report runs itself and is idempotent per window.

---

## Stage 3 · React to data, not the clock (asset-based) · `stage-3-assets/`

**The need:** a timer only *hopes* the upstream data landed. The company wants runs that fire **when the data is actually ready**.

| # | Topic | Why | Status |
|---|---|---|---|
| 09 | **Assets & data-aware scheduling** (end-to-end) → [note](stage-3-assets/09-assets.md) | run the report the moment the table updates | 🟡 |
| 10 | **Asset logic** — `AssetAll`/`AssetAny`, aliases → [note](stage-3-assets/10-asset-logic.md) | run only when *several* inputs are ready | 🟡 |
| 11 | **Event-driven** — watchers, message queues → [note](stage-3-assets/11-event-driven.md) | trigger from outside events, not just Airflow | 🟡 |

**Milestone:** the pipeline is a graph of data dependencies, not a wall of cron lines.

---

## Stage 4 · Connect the warehouse & add brains (the ELT era) · `stage-4-warehouse/`

**The need:** real data in BigQuery, pipelines that make decisions and scale over many inputs. (ELT is ~90% of what companies use Airflow for.)

| # | Topic | Why | Status |
|---|---|---|---|
| 12 | **Connections & Hooks** → [note](stage-4-warehouse/12-hooks.md) | talk to BigQuery/DBs with creds kept out of code | 🟡 |
| 13 | **BigQuery ground-up → advanced** → [note](stage-4-warehouse/13-bigquery.md) | the warehouse: cost, partitioning, idempotent loads | ✅ |
| 14 | **Branching & trigger rules** → [note](stage-4-warehouse/14-branching-trigger-rules.md) | pipelines that choose a path and survive skips | ✅ |
| 15 | **Dynamic task mapping** → [note](stage-4-warehouse/15-dynamic-task-mapping.md) | fan out over N files/regions decided at run time | ✅ |
| 24 | **XCom backends & ObjectStorage** → [note](stage-4-warehouse/24-xcom-backends-objectstorage.md) | pass large data by reference, keep it out of the metadata DB | 🟡 |

**Milestone:** StackPulse runs real cost-capped BigQuery ELT that branches and scales.

---

## Stage 5 · Trust it, and get paged when it breaks · `stage-5-quality/`

**The need:** bad data must not reach customers, and a failure must reach a human.

| # | Topic | Why | Status |
|---|---|---|---|
| 16 | **Data quality** — SQL checks, circuit breaker → [note](stage-5-quality/16-data-quality.md) | stop a bad number before the VP sees it | 🟡 |
| 17 | **Alerting & callbacks** — `on_failure_callback`, Slack → [note](stage-5-quality/17-alerting-callbacks.md) | know within minutes, not days | 🟡 |
| 18 | **Observability** — logging, metrics, lineage → [note](stage-5-quality/18-observability.md) | see *why* it broke | 🟡 |

**Milestone:** a broken run self-checks, halts, and alerts on-call.

---

## Stage 6 · Scale & harden for production · `stage-6-scale/`

**The need:** many DAGs, many teams, cost and reliability under control.

| # | Topic | Why | Status |
|---|---|---|---|
| 19 | **Sensors & deferrable operators** → [note](stage-6-scale/19-sensors-deferrable.md) | wait for things without burning workers | 🟡 |
| 20 | **Variables & Secrets backends** → [note](stage-6-scale/20-variables-secrets.md) | config and secrets done right at scale | 🟡 |
| 21 | **Executors & concurrency** — pools, priority → [note](stage-6-scale/21-executors-concurrency.md) | keep 100 DAGs from trampling each other | 🟡 |
| 22 | **DAG versioning & CI/CD** → [note](stage-6-scale/22-dag-versioning-cicd.md) | ship safely; every run pinned to its code | 🟡 |
| 23 | **dbt via Cosmos, push-down ELT** → [note](stage-6-scale/23-dbt-cosmos.md) | integrate the modern transform layer | 🟡 |
| 25 | **Cross-DAG dependencies** → [note](stage-6-scale/25-cross-dag-dependencies.md) | connect DAGs: `TriggerDagRunOperator`, `ExternalTaskSensor`, or assets | 🟡 |
| 26 | **Setup & teardown** → [note](stage-6-scale/26-setup-teardown.md) | spin up a resource and always tear it down, even on failure | 🟡 |
| 27 | **Reliability & deadlines** → [note](stage-6-scale/27-reliability-deadlines.md) | timeouts, backoff, Deadline Alerts, `depends_on_past` | 🟡 |
| 28 | **Dependency isolation** → [note](stage-6-scale/28-dependency-isolation.md) | run a task in its own venv/interpreter to end version fights | 🟡 |
| 29 | **Container tasks** → [note](stage-6-scale/29-container-tasks.md) | run a task as a Docker container or Kubernetes pod | 🟡 |
| 30 | **Parsing performance** → [note](stage-6-scale/30-parsing-performance.md) | keep top-level code cheap; tune the DAG processor & scheduler | 🟡 |
| 31 | **Multi-tenancy** → [note](stage-6-scale/31-multi-tenancy.md) | many teams on one Airflow: bundles, RBAC, queues | 🟡 |

**Milestone:** the platform survives real load, real teams, and real deploys.

---

## Capstone · The StackPulse platform · `capstone/`

Wire every layer into **one** pipeline on `bigquery-public-data.stackoverflow`: asset-triggered, cost-controlled BigQuery ELT, branching on data, dynamic-mapped over sources, quality-gated, Slack-alerting, tested, and CI-gated. The thing a real data team would actually run. → [note](capstone/capstone.md)

## Practicals · `practicals/`

| # | Topic | Status |
|---|---|---|
| P1 | **BigQuery hello** — count + top-N, cost-capped → [note](practicals/P1-bigquery-hello.md) | ✅ |
| P2 | **Parametrized dynamic load** — Params + dynamic mapping, incremental by date → [note](practicals/P2-parametrized-dynamic-load.md) | 🟡 |
| P3 | **Medallion via BigQuery stored procedures** — raw→bronze→gold, CALL per layer, DQ gate (spec only) → [note](practicals/P3-bq-stored-proc-medallion.md) | ✅ |
| P4 | **Backfilling the medallion** — date-parametrize P3, `airflow backfill create`, prove idempotent reprocessing (spec only) → [note](practicals/P4-medallion-backfill.md) | 🟡 |

---

## Where you are right now

Done: **00–06, 13, 14, 15, P1** (basics, scheduling start, and the warehouse trio). Everything else is **written and ready** (🟡) — notes + CI-green scaffolds exist; you just haven't done the build yet. The fastest path that respects the company story: finish **Stage 2** (07 backfill → 08 timetables), then **Stage 3 assets** (09 → 10 → 11) — the part that turns a pile of cron jobs into a real data platform — then Stage 5 quality/alerting so nothing bad reaches customers.

Sources:
[Astronomer — Airflow 101 learning path](https://academy.astronomer.io/path/airflow-101),
[Airflow ETL/ELT use cases](https://airflow.apache.org/use-cases/etl_analytics/),
[Airflow production best practices — idempotency, quality, assets](https://www.astronomer.io/airflow/use-cases/)
