# Airflow 3 — Daily Bytes

Apache Airflow **3.3**. Learn it in **~20 minutes a day, every day**. Each item
below is a **byte**: one small chunk you finish in one sitting. Do a byte, tick it,
take the points, keep the streak alive.

## Scoreboard

| | |
|---|---|
| **Total points** | 170 |
| **Current streak** | 0 days — start today |
| **Longest streak** | 3 days |
| **Last active** | 2026-09-06 |

**Points:** a *learn* byte = **10**, a *build-a-DAG* byte = **20** (counts only when
its commit is CI-green). **Streak** = consecutive days with at least one byte done.
Update this scoreboard when you finish a byte.

**How a byte works:** open the topic note, read only that byte's section, run the
tiny example or build the small thing, tick the box. No note repeats these rules —
they live here only.

---

## Done ✅  (170 pts)

- [x] **01 · TaskFlow** — `@dag`/`@task`, return→XCom, `multiple_outputs`
- [x] **02 · Classic operators** — `>>`, `chain`, `chain_linear`, `cross_downstream`
- [x] **03 · TaskGroups** — `@task_group`, nesting, `group_id`
- [x] **🔷 P1 · BigQuery hello** — count + top-N stations on `austin_bikeshare`, cost-capped

Reference pages (read anytime): [xcom-basics](xcom-basics.md) · [gcp-project](gcp-project.md)

---

## Now → next bytes

### 04 · Branching & trigger rules → [note](04-branching-trigger-rules.md)
- [ ] **4.1** `@task.branch` — read §1, run the tiny branch example · *10*
- [ ] **4.2** `@task.short_circuit` — read §2, run the guard example · *10*
- [ ] **4.3** `TriggerRule` + the join gotcha — read §3–§4 · *10*
- [ ] **4.4** Build the BigQuery branching DAG — §7 spec, CI-green · *20*

### 05 · Params + Jinja + context → note (to be written)
- [ ] **5.1** `Param` — declare params, trigger a DAG with config · *10*
- [ ] **5.2** Jinja templates — `{{ ds }}`, `{{ params.x }}`, `template_fields` · *10*
- [ ] **5.3** `get_current_context` — read run info inside a task · *10*
- [ ] **5.4** Build a parametrized BigQuery DAG · *20*

### 06 · Dynamic task mapping (deferred from 04) → [note](06-dynamic-task-mapping.md)
- [ ] **6.1** `.expand` + `.partial` — read §1–§2, run the mapped example · *10*
- [ ] **6.2** Reduce — read §7, add the collector task · *10*
- [ ] **6.3** `.expand_kwargs` / `.zip` / `.map` — read §4–§6 · *10*
- [ ] **6.4** Build a mapped BigQuery DAG · *20*

### 🔷 P2 · Parametrized dynamic load → practical (to be written)
- [ ] Applies 04–06 on `google_analytics_sample.ga_sessions_*`: param date range,
  dynamic-map over date shards, branch on empty shard. *(broken into bytes when reached)*

---

## The road ahead (each topic → ~3–4 bytes, broken out when you reach it)

**Phase B — Scheduling & data-awareness:** 07 Schedules & intervals · 08 Timetables ·
09 Backfill · 🔷 P3 Incremental daily + backfill · 10 Assets · 11 Asset logic ·
12 Event-driven · 🔷 P4 Asset-driven BQ pipeline

**Phase C — Data passing, connections, sensors:** 13 XCom deep · 14 XCom backend +
ObjectStorage · 15 Connections + Hooks · 🔷 P5 BQ→GCS · 16 Variables + Secrets ·
17 Sensors · 18 Deferrable + Triggerer · 🔷 P6 Secrets + deferrable sensor

**Phase D — Isolation, containers, cross-DAG:** 19 Dependency isolation · 20 Container
tasks · 21 Cross-DAG · 🔷 P7 Isolated transform + cross-DAG · 22 Setup/teardown

**Phase E — Architecture & scaling:** 23 Architecture · 24 Executors · 25 Concurrency
hierarchy · 🔷 P8 Fan-out + concurrency · 26 Parsing performance · 27 DAG versioning ·
28 Reliability · 🔷 P9 Hardening the BQ pipeline

**Phase F — Operations & production quality:** 29 Alerting · 30 Observability ·
31 Data quality · 🔷 P10 Alerting + self-checks · 32 Testing + CI/CD · 33 dbt via
Cosmos · 34 Warehouse push-down ELT · 🔷 P11 DQ gate + dbt on BigQuery

**Phase G — Capstone:** 35 Multi-tenancy · 🔶 36 Capstone (the full BigQuery pipeline)

**Every 3 topics comes a 🔷 practical** on a real `bigquery-public-data` dataset,
each adding a layer to one growing pipeline:

```
bigquery-public-data source
  → dynamic-mapped, partition-by-date extract      (P1–P3)
  → GCS raw zone via ObjectStoragePath             (P5)
  → asset-triggered transform in BigQuery          (P4)
  → dbt staging + marts (Silver/Gold)              (P11)
  → SQL data-quality gate + Slack alerting         (P10–P11)
  → retries/deadlines, pools, versioning, CI       (P8–P11)
```

---

## GCP setup (done once — already wired)

Connection `google_cloud_default` uses your service-account key (kept outside the
repo). Full walk-through: [P1-bigquery-hello.md](P1-bigquery-hello.md). Cost rule:
always cap queries (`maximumBytesBilled`), no `SELECT *`, never Cloud Composer.
