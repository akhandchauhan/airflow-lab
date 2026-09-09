# Airflow 3 — Daily Bytes

Apache Airflow **3.3**. Learn it in **~20 minutes a day, every day**. Each item
below is a **byte**: one small chunk you finish in one sitting. Do a byte, tick it,
take the points, keep the streak alive.

## Scoreboard

|                    |                      |
| ------------------ | -------------------- |
| **Total points**   | 230                  |
| **Current streak** | 2 days               |
| **Longest streak** | 3 days               |
| **Last active**    | 2026-09-09           |

**Points:** a _learn_ byte = **10**, a _build-a-DAG_ byte = **20** (counts only when
its commit is CI-green). **Streak** = consecutive days with at least one byte done.
Update this scoreboard when you finish a byte.

**How a byte works:** open the topic note, read only that byte's section, run the
tiny example or build the small thing, tick the box. No note repeats these rules —
they live here only.

---

## Done ✅ (170 pts)

- [x] **01 · TaskFlow** — `@dag`/`@task`, return→XCom, `multiple_outputs`
- [x] **02 · Classic operators** — `>>`, `chain`, `chain_linear`, `cross_downstream`
- [x] **03 · TaskGroups** — `@task_group`, nesting, `group_id`
- [x] **🔷 P1 · BigQuery hello** — count + top-N stations on `austin_bikeshare`, cost-capped

Reference pages (read anytime): [xcom-basics](xcom-basics.md) · [gcp-project](gcp-project.md) · [unnest](unnest.md)

---

## Now → next bytes

### 🎯 Session 04 · Give the Pipeline a Brain — branching & trigger rules → [note](04-branching-trigger-rules.md)

_Story: the Product Health report ran on an empty table and told the VP "0 unanswered questions". Give the pipeline a brain._

- [x] **4.1** `@task.branch` — read §1, run the branch snippet · _10_ ✅ (+ §1 XCom challenge)
- [x] **4.2** `@task.short_circuit` — read §2, the guard that stops empty runs · _10_ ✅ (fix `owner` casing)
- [x] **4.3** `TriggerRule` + the 2am trap — read §3–§4 · _10_
- [~] **4.4** ~~Build `s4_product_health`~~ — **skipped** (concepts done in 4.1–4.3)

> **Current mode: slower + simpler.** Short bytes, one tiny idea each, **plain DAGs
> — no BigQuery** for a while, until it feels easy.

### 🎯 Session 05 · When a Task Falls Over — retries → [note](05-retries.md)

_Story: a task hit a 1-second network blip at 3am and paged you — a retry would have fixed it on its own._

Starter files are ready in `dags/s5/` — just fill in the task body.

- [x] **5.1** `retries` — read §1, edit `dags/s5/s5_task1.py` · _10_
- [x] **5.2** `retry_delay` — read §2, edit `dags/s5/s5_task2.py` · _10_
- [x] **5.3** Build in `dags/s5/s5_task3.py` — a plain DAG that recovers, §4 · _20_

### 🎯 Session 06 · Give the DAG a Dial — params → [note](06-params.md)

_Story: the target country was hardcoded — every change meant editing the file and redeploying. A param makes it a dial anyone sets at trigger time._

Starter files are ready in `dags/s6/` — just fill in the task body.

- [ ] **6.1** Declare + read a param — read §1–§3, edit `dags/s6/s6_task1.py` · _10_
- [ ] **6.2** Pass a value with `--conf` — read §2, edit `dags/s6/s6_task2.py` · _10_
- [ ] **6.3** Build in `dags/s6/s6_task3.py` — two params + validation, §4 · _20_

_(Jinja templating + `get_current_context` deep-dive deferred to a later session — keeping bytes tiny.)_

### 07 · Dynamic task mapping (deferred from 04) → [note](07-dynamic-task-mapping.md)

- [ ] **7.1** `.expand` + `.partial` — read §1–§2, run the mapped example · _10_
- [ ] **7.2** Reduce — read §7, add the collector task · _10_
- [ ] **7.3** `.expand_kwargs` / `.zip` / `.map` — read §4–§6 · _10_
- [ ] **7.4** Build a mapped BigQuery DAG · _20_

### 🔷 P2 · Parametrized dynamic load → practical (to be written)

- [ ] Applies 04–06 on `google_analytics_sample.ga_sessions_*`: param date range,
      dynamic-map over date shards, branch on empty shard. _(broken into bytes when reached)_

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
