# Airflow 3 — Daily Bytes

Apache Airflow **3.3**, learned the way a company actually adopts it — prove it works,
build basic DAGs, put them on a clock, react to data, connect the warehouse, trust it,
scale it. **The full path & the _why_ of the order live in [CURRICULUM.md](CURRICULUM.md).**
This file is the **tracker**: scoreboard + what's done. ~20 minutes a day.

## Scoreboard

|                    |            |
| ------------------ | ---------- |
| **Total points**   | 290        |
| **Current streak** | 1 day      |
| **Longest streak** | 3 days     |
| **Last active**    | 2026-09-15 |

**Points:** each session ≈ **10 per concept** + **20 for its build DAG** (build counts
when its commit is CI-green). **Streak** = consecutive days you do something. Update
the scoreboard when you finish a session.

**How it works:** open the session note, read it, run the reference DAG, do the build
in its `dags/s<N>/` scaffold, tick the session below. Rules live here only — the notes
don't repeat them.

---

## Done ✅

- [x] **00 · First DAG & testing** — DAG model, `dag.test()`, integrity gates → [note](00-first-dag-testing.md)
- [x] **01 · TaskFlow** — `@dag`/`@task`, return→XCom → [note](01-taskflow-foundations.md)
- [x] **02 · Classic operators** — `>>`, `chain`, `cross_downstream` → [note](02-classic-operators.md)
- [x] **03 · TaskGroups** — `@task_group`, nesting, `group_id` → [note](03-task-groups.md)
- [x] **🔷 P1 · BigQuery hello** — count + top-N, cost-capped → [note](P1-bigquery-hello.md)
- [x] **04 · Give the Pipeline a Brain** — branching & trigger rules → [note](04-branching-trigger-rules.md) _(build skipped)_
- [x] **05 · When a Task Falls Over** — retries → [note](05-retries.md)
- [x] **06 · Inside the Warehouse** — BigQuery ground-up → advanced → [note](06-bigquery.md)
- [x] **07 · Params** — input at trigger time, validation → [note](07-params.md)

Reference pages (read anytime): [xcom-basics](xcom-basics.md) · [gcp-project](gcp-project.md) · [unnest](unnest.md) · [stage1-recap](stage1-recap.md)

---

## Now → next

- [x] **08 · Dynamic task mapping** — `.expand` / `.partial`, fan out at runtime; plain DAGs, files in `dags/s8/` → [note](08-dynamic-task-mapping.md)
- [x] **09 · Schedules & intervals** — `schedule`, data interval, `catchup`; plain DAGs, files in `dags/s9/` → [note](09-schedules.md) · _Phase B starts_
- [ ] **12 · Assets (data-aware scheduling)** — react to data, not the clock; end-to-end; files in `dags/s12/` → [note](12-assets.md) · _Stage 3_
- [ ] **🔷 P2 · Parametrized dynamic load** — applies 04–08 on `ga_sessions_*` (to be written)

---

## The road ahead

The full staged plan — Stage 0 (does it work?) → 1 basic DAGs → 2 cron → 3 assets →
4 warehouse & brains → 5 quality & alerting → 6 scale & harden → capstone — lives in
**[CURRICULUM.md](CURRICULUM.md)**, with the business reason for each stage.

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
