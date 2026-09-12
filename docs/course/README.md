# Airflow 3 — Daily Bytes

Apache Airflow **3.3**. Learn it in **~20 minutes a day, every day**. Each item
below is a **byte**: one small chunk you finish in one sitting. Do a byte, tick it,
take the points, keep the streak alive.

## Scoreboard

|                    |            |
| ------------------ | ---------- |
| **Total points**   | 260        |
| **Current streak** | 3 days     |
| **Longest streak** | 3 days     |
| **Last active**    | 2026-09-10 |

**Points:** each session ≈ **10 per concept** + **20 for its build DAG** (build counts
when its commit is CI-green). **Streak** = consecutive days you do something. Update
the scoreboard when you finish a session.

**How it works:** open the session note, read it, run the reference DAG, do the build
in its `dags/s<N>/` scaffold, tick the session below. Rules live here only — the notes
don't repeat them.

---

## Done ✅

- [x] **01 · TaskFlow** — `@dag`/`@task`, return→XCom → [note](01-taskflow-foundations.md)
- [x] **02 · Classic operators** — `>>`, `chain`, `cross_downstream` → [note](02-classic-operators.md)
- [x] **03 · TaskGroups** — `@task_group`, nesting, `group_id` → [note](03-task-groups.md)
- [x] **🔷 P1 · BigQuery hello** — count + top-N, cost-capped → [note](P1-bigquery-hello.md)
- [x] **04 · Give the Pipeline a Brain** — branching & trigger rules → [note](04-branching-trigger-rules.md) _(build skipped)_
- [x] **05 · When a Task Falls Over** — retries → [note](05-retries.md)
- [x] **06 · Inside the Warehouse** — BigQuery ground-up → advanced → [note](06-bigquery.md)
- [x] **07 · Params** — input at trigger time, validation → [note](07-params.md)

Reference pages (read anytime): [xcom-basics](xcom-basics.md) · [gcp-project](gcp-project.md) · [unnest](unnest.md)

---

## Now → next

- [ ] **08 · Dynamic task mapping** — `.expand` / `.partial`, fan out at runtime; plain DAGs, files in `dags/s8/` → [note](08-dynamic-task-mapping.md)
- [ ] **🔷 P2 · Parametrized dynamic load** — applies 04–08 on `ga_sessions_*` (to be written)

---

## The road ahead (each topic → ~3–4 bytes, broken out when you reach it)

**Phase B — Scheduling & data-awareness:** 07 Schedules & intervals · 08 Timetables ·
09 Backfill · 🔷 P3 Incremental daily + backfill · 10 Assets · 11 Asset logic ·
12 Event-driven · 🔷 P4 Asset-driven BQ pipeline

**Phase C — Data passing, connections, sensors:** 13 XCom deep · 14 XCom backend +
ObjectStorage · [15 Connections + Hooks](15-hooks.md) · 🔷 P5 BQ→GCS ·
16 Variables + Secrets · 17 Sensors · 18 Deferrable + Triggerer ·
🔷 P6 Secrets + deferrable sensor

**Phase D — Isolation, containers, cross-DAG:** 19 Dependency isolation · 20 Container
tasks · 21 Cross-DAG · 🔷 P7 Isolated transform + cross-DAG · 22 Setup/teardown

**Phase E — Architecture & scaling:** 23 Architecture · 24 Executors · 25 Concurrency
hierarchy · 🔷 P8 Fan-out + concurrency · 26 Parsing performance · 27 DAG versioning ·
28 Reliability · 🔷 P9 Hardening the BQ pipeline

**Phase F — Operations & production quality:** 29 Alerting · 30 Observability ·
31 Data quality · 🔷 P10 Alerting + self-checks · 32 Testing + CI/CD · 33 dbt via
Cosmos · 34 Warehouse push-down ELT · 🔷 P11 DQ gate + dbt on BigQuery

**Phase G — Capstone:** 35 Multi-tenancy · 🔶 36 Capstone (the full BigQuery pipeline)

## 🎬 The project: Stack Overflow Product Health

You're the data engineer on Stack Overflow's analytics team. Every session adds a
layer to **one** pipeline on `bigquery-public-data.stackoverflow` (questions,
answers, users, tags) — each one opens with a real on-call scenario, and you
build the fix.

```
Stack Overflow (posts_questions, posts_answers, users, tags)
  → daily health brain: unanswered backlog, answer rate, tag trends   (S4–S6)
  → parametrized + incremental by date                                (P2–P3)
  → GCS raw zone + asset-triggered marts                              (P4–P5)
  → dbt staging + Gold tables; DQ gate + Slack alert                  (P10–P11)
  → retries/deadlines, pools, DAG versioning, CI gating               (P8–P11)
```

The capstone wires every layer into one alerting, tested, cost-controlled pipeline.

---

## GCP setup (done once — already wired)

Connection `google_cloud_default` uses your service-account key (kept outside the
repo). Full walk-through: [P1-bigquery-hello.md](P1-bigquery-hello.md). Cost rule:
always cap queries (`maximumBytesBilled`), no `SELECT *`, never Cloud Composer.
