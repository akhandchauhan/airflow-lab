# Airflow 3 Weekend Course — Progress Index

Apache Airflow **3.3**. One hour Saturday + one hour Sunday. Build-it-yourself
specs, no full solutions. Airflow 3 only.

**Rhythm:** every **3 concept sessions** are followed by **1 practical (🔷)**
that applies all three on a real **BigQuery public dataset** (`bigquery-public-data.*`)
in your own GCP project. Concepts teach the mechanism; practicals make it real
against production-shaped data.

Concept ritual: **concept -> API + example -> complete runnable reference -> build
spec -> production tip -> verify & push**.
Practical ritual: **real dataset -> build spec -> run against BigQuery -> verify
rows + bytes billed -> push**.

Artifacts: concept note `docs/course/WW-topic.md` (+ local `Documents/AIRFLOW/course/`);
your DAG in `dags/task-N/` (session N in its own subfolder — Airflow parses
`dags/` recursively), must pass integrity tests + CI. Tick each item when its
CI-green commit lands.

---

## GCP setup (do once, before P1)

You have GCP creds - wire Airflow to BigQuery:

1. **Provider:** add `apache-airflow-providers-google` to `requirements.txt`, `pip install` it.
2. **Service account:** GCP console -> IAM -> service account with **BigQuery Job User** + **BigQuery Data Viewer**. Download the JSON key.
3. **Connection:** Airflow connection `google_cloud_default` (type *Google Cloud*) with the key JSON + project id - or env var `AIRFLOW_CONN_GOOGLE_CLOUD_DEFAULT`.
4. **Cost safety:** public-dataset queries bill **your** project for bytes scanned (free tier 1 TiB/mo). Always: no `SELECT *` on big tables; add `LIMIT`/aggregate; prefer small tables (`austin_bikeshare`, `usa_names`); set `maximum_bytes_billed` (~1 GB) on the operator. Never create a Cloud Composer environment.

Full walk-through lands in **P1**.

---

## Phase A - Authoring surface
- [x] **01 - TaskFlow** - `@dag`/`@task`, return->XCom, `multiple_outputs`
- [x] **02 - Classic operators** - `>>`, `chain`, `chain_linear`, `cross_downstream`
- [ ] **03 - TaskGroups** - `@task_group`, nesting, `group_id`
- [ ] **🔷 P1 - BigQuery hello** - TaskFlow + TaskGroups on `bigquery-public-data.austin_bikeshare.bikeshare_trips` (row count + top-N stations, grouped)
- [ ] **04 - Dynamic task mapping** - `.expand`, `.partial`, `.expand_kwargs`, `.map`, `.zip`
- [ ] **05 - Params + Jinja + context** - `Param`, `template_fields`, `get_current_context`
- [ ] **06 - Branching + trigger rules** - `@task.branch`, `@task.short_circuit`, `TriggerRule`
- [ ] **🔷 P2 - Parametrized dynamic load** - dynamic-map over date shards of `google_analytics_sample.ga_sessions_*`, param date range, branch on empty shard

## Phase B - Scheduling & data-awareness
- [ ] **07 - Schedules & intervals** - cron, data intervals, `logical_date`, `catchup`
- [ ] **08 - Timetables** - `CronTriggerTimetable` vs `CronDataIntervalTimetable`, custom
- [ ] **09 - Backfill** - `airflow backfill create`, `--reprocess-behavior`, `max_active_runs`
- [ ] **🔷 P3 - Incremental daily + backfill** - daily incremental load from the GA sample by date partition, backfill one week
- [ ] **10 - Assets** - `@asset`, `Asset`, `outlets`/`inlets`, asset-aware `schedule=[...]`
- [ ] **11 - Asset logic** - `AssetAll`/`AssetAny`, `AssetAlias`, partitions
- [ ] **12 - Event-driven** - `AssetWatcher`, message-queue triggers, `AssetOrTimeSchedule`
- [ ] **🔷 P4 - Asset-driven BQ pipeline** - producer DAG materializes a BQ summary table (asset); consumer DAG asset-triggered

## Phase C - Data passing, connections, sensors
- [ ] **13 - XCom deep** - keys, `multiple_outputs`, scope
- [ ] **14 - XCom backend + ObjectStorage** - `ObjectStoragePath`, large payloads to GCS
- [ ] **15 - Connections + Hooks** - `BaseHook.get_connection`, `BigQueryHook`, `conn_id`
- [ ] **🔷 P5 - Connection-driven BQ -> GCS** - query via `google_cloud_default`, write results to GCS with `ObjectStoragePath`, pass path via XCom
- [ ] **16 - Variables + Secrets backends** - `Variable`, backend search order, Jinja access
- [ ] **17 - Sensors** - poke vs reschedule, `BigQueryTablePartitionExistenceSensor`, `@task.sensor`
- [ ] **18 - Deferrable + Triggerer** - `self.defer`, `BaseTrigger`, deferrable BQ sensor
- [ ] **🔷 P6 - Secrets + deferrable sensor** - creds via secrets backend; deferrable sensor waits for a BQ partition before loading

## Phase D - Isolation, containers, cross-DAG
- [ ] **19 - Dependency isolation** - `PythonVirtualenvOperator`, `ExternalPythonOperator`
- [ ] **20 - Container tasks** - `DockerOperator`, `KubernetesPodOperator`
- [ ] **21 - Cross-DAG** - `TriggerDagRunOperator`, Assets vs `ExternalTaskSensor`
- [ ] **🔷 P7 - Isolated transform + cross-DAG** - BQ transform in an isolated task; triggers a downstream reporting DAG
- [ ] **22 - Setup/teardown** - `@setup`, `@teardown`, `.as_teardown`

## Phase E - Architecture & scaling
- [ ] **23 - Architecture** - Scheduler, DAG Processor, API Server, Task Execution API, bundles
- [ ] **24 - Executors** - Local/Celery/Kubernetes/Edge + multiple-executor routing
- [ ] **25 - Concurrency hierarchy** - `parallelism` -> pools -> `max_active_tasks` -> `max_active_runs` -> `priority_weight`
- [ ] **🔷 P8 - Fan-out + concurrency control** - fan out many BQ queries, cap with a pool + `max_active_tasks`; setup/teardown a scratch BQ dataset
- [ ] **26 - Parsing performance** - top-level code, `min_file_process_interval`, scheduler HA
- [ ] **27 - DAG versioning + bundles**
- [ ] **28 - Reliability** - retries, timeouts, Deadline Alerts, `depends_on_past`
- [ ] **🔷 P9 - Hardening the BQ pipeline** - retries/timeouts/deadlines, fix a top-level BQ call, observe DAG versioning

## Phase F - Operations & production quality
- [ ] **29 - Alerting** - `on_failure_callback`, `BaseNotifier` (Slack), listeners, cluster policies
- [ ] **30 - Observability** - remote logging, StatsD/OpenTelemetry, health checks, OpenLineage
- [ ] **31 - Data quality** - SQL check operators, `ShortCircuitOperator` circuit breaker
- [ ] **🔷 P10 - Alerting + self-checks on BQ load** - Slack on failure; self-check task confirming the loaded table's row count
- [ ] **32 - Testing + CI/CD** - `dag.test()`, integrity tests, ruff AIR3, mypy, GH Actions
- [ ] **33 - dbt via Cosmos** - `DbtDag`/`DbtTaskGroup` on BigQuery
- [ ] **34 - Warehouse push-down ELT** - `BigQueryInsertJobOperator`, orchestrate-don't-compute
- [ ] **🔷 P11 - DQ gate + dbt on BigQuery** - SQL checks as a circuit breaker before load; dbt staging/marts on BQ; CI-gated

## Phase G - Multi-tenancy & capstone
- [ ] **35 - Multi-tenancy** - teams, DAG-bundle ownership, RBAC/auth managers, `queue`
- [ ] **🔶 36 - Capstone** - the full BigQuery pipeline end to end (below)

---

## The through-line project (a real BigQuery pipeline)

Each practical adds a layer to **one** pipeline in your own GCP project, sourced
from `bigquery-public-data`:

```
bigquery-public-data source (GA sample / bikeshare / stackoverflow)
  -> dynamic-mapped, partition-by-date extract           (P1-P3)
  -> GCS raw zone via ObjectStoragePath                   (P5)
  -> asset-triggered transform in BigQuery                (P4)
  -> dbt staging + marts on BigQuery (Silver/Gold)        (P11)
  -> SQL data-quality circuit breaker + Slack alerting    (P10-P11)
  -> retries/deadlines, pools, DAG versioning, CI gating  (P8-P11)
```

Capstone (36) wires every layer into one asset-linked, tested, alerting,
cost-controlled BigQuery pipeline.
