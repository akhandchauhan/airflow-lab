# Airflow 3 — Daily Bytes

Apache Airflow **3.3**, learned the way a company actually adopts it — prove it works, build basic DAGs, put them on a clock, react to data, connect the warehouse, trust it, scale it. **The full path & the _why_ of the order live in [CURRICULUM.md](CURRICULUM.md).** This file is the **tracker**: scoreboard + what's done. ~20 minutes a day.

## Scoreboard

|                    |            |
| ------------------ | ---------- |
| **Total points**   | 290        |
| **Current streak** | 1 day      |
| **Longest streak** | 3 days     |
| **Last active**    | 2026-09-15 |

**Points:** each session ≈ **10 per concept** + **20 for its build DAG** (build counts when its commit is CI-green). **Streak** = consecutive days you do something. Update the scoreboard when you finish a session.

**How it works:** notes and DAGs are now grouped into **phase folders**. Open the session note under `docs/course/<phase>/`, read it, run the reference DAG, do the build in its scaffold under `dags/<phase>/s<N>/`, tick the session below. Rules live in `RULES.md` — the notes don't repeat them.

---

## The path (tick as you go)

Legend: `[x]` = done · `[ ]` = written & ready, not finished yet.

### Stage 0 · Day one — `stage-0-day-one/`
- [x] **00 · First DAG & testing** — DAG model, `dag.test()`, integrity gates → [note](stage-0-day-one/00-first-dag-testing.md)

### Stage 1 · Basic DAGs — `stage-1-basic-dags/`
- [x] **01 · TaskFlow** — `@dag`/`@task`, return→XCom → [note](stage-1-basic-dags/01-taskflow-foundations.md)
- [x] **02 · Classic operators** — `>>`, `chain`, `cross_downstream` → [note](stage-1-basic-dags/02-classic-operators.md)
- [x] **03 · TaskGroups** — `@task_group`, nesting, `group_id` → [note](stage-1-basic-dags/03-task-groups.md)
- [x] **04 · Retries** — flaky calls shouldn't page you → [note](stage-1-basic-dags/04-retries.md)
- [x] **05 · Params** — input at trigger time, validation → [note](stage-1-basic-dags/05-params.md)
- 📄 **Stage 1 recap (all code in one file, incl. Jinja)** → [recap](stage-1-basic-dags/stage1-recap.md)

### Stage 2 · Put it on the clock — `stage-2-scheduling/`
- [x] **06 · Schedules & intervals** — `schedule`, data interval, `catchup` → [note](stage-2-scheduling/06-schedules.md)
- [ ] **07 · Backfill** — fill history on purpose, safely → [note](stage-2-scheduling/07-backfill.md)
- [ ] **08 · Timetables** — cadences cron can't express → [note](stage-2-scheduling/08-timetables.md)

### Stage 3 · React to data — `stage-3-assets/`
- [ ] **09 · Assets (data-aware scheduling)** — run when data lands, not on a timer → [note](stage-3-assets/09-assets.md)
- [ ] **10 · Asset logic** — `AssetAll`/`AssetAny`, aliases → [note](stage-3-assets/10-asset-logic.md)
- [ ] **11 · Event-driven** — watchers, message queues → [note](stage-3-assets/11-event-driven.md)

### Stage 4 · Warehouse & brains — `stage-4-warehouse/`
- [ ] **12 · Connections & Hooks** — creds kept out of code → [note](stage-4-warehouse/12-hooks.md)
- [x] **13 · BigQuery** — ground-up → advanced, cost-capped → [note](stage-4-warehouse/13-bigquery.md)
- [x] **14 · Branching & trigger rules** — choose a path, survive skips → [note](stage-4-warehouse/14-branching-trigger-rules.md) _(build skipped)_
- [x] **15 · Dynamic task mapping** — `.expand`/`.partial`, fan out at runtime → [note](stage-4-warehouse/15-dynamic-task-mapping.md)

### Stage 5 · Trust it, get paged — `stage-5-quality/`
- [ ] **16 · Data quality** — SQL checks, circuit breaker → [note](stage-5-quality/16-data-quality.md)
- [ ] **17 · Alerting & callbacks** — `on_failure_callback`, Slack → [note](stage-5-quality/17-alerting-callbacks.md)
- [ ] **18 · Observability** — logging, metrics, lineage → [note](stage-5-quality/18-observability.md)

### Stage 6 · Scale & harden — `stage-6-scale/`
- [ ] **19 · Sensors & deferrable operators** — wait without burning workers → [note](stage-6-scale/19-sensors-deferrable.md)
- [ ] **20 · Variables & Secrets backends** — config & secrets at scale → [note](stage-6-scale/20-variables-secrets.md)
- [ ] **21 · Executors & concurrency** — pools, priority → [note](stage-6-scale/21-executors-concurrency.md)
- [ ] **22 · DAG versioning & CI/CD** — ship safely, pin each run → [note](stage-6-scale/22-dag-versioning-cicd.md)
- [ ] **23 · dbt via Cosmos** — the modern transform layer → [note](stage-6-scale/23-dbt-cosmos.md)

### Capstone — `capstone/`
- [ ] **Capstone · The StackPulse platform** — wire every layer into one pipeline → [note](capstone/capstone.md)

### Practicals — `practicals/`
- [x] **🔷 P1 · BigQuery hello** — count + top-N, cost-capped → [note](practicals/P1-bigquery-hello.md)
- [ ] **🔷 P2 · Parametrized dynamic load** — Params + dynamic mapping, incremental by date → [note](practicals/P2-parametrized-dynamic-load.md)

**Reference pages** (`reference/`, read anytime): [xcom-basics](reference/xcom-basics.md) · [gcp-project](reference/gcp-project.md) · [unnest](reference/unnest.md) · [etl-in-gcp](reference/etl-in-gcp.md)

---

## 🎬 The project: Stack Overflow Product Health

You're the data engineer on Stack Overflow's analytics team. From Stage 4 on, every session adds a layer to **one** pipeline on `bigquery-public-data.stackoverflow` (questions, answers, users, tags).

```
Stack Overflow (posts_questions, posts_answers, users, tags)
  → daily health brain: unanswered backlog, answer rate, tag trends   (Stage 4)
  → asset-triggered marts + quality gate + Slack alert                (Stages 3, 5)
  → retries/deadlines, versioning, CI gating, dbt                     (Stage 6)
```

The capstone wires every layer into one alerting, tested, cost-controlled pipeline.

---

## GCP setup (done once — already wired)

Connection `google_cloud_default` uses your service-account key (kept outside the repo). Full walk-through: [practicals/P1-bigquery-hello.md](practicals/P1-bigquery-hello.md). Cost rule: always cap queries (`maximumBytesBilled`), no `SELECT *`, never Cloud Composer.
