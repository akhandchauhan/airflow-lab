# Airflow 3 Weekend Course — Progress Index

Apache Airflow **3.3**. One hour Saturday + one hour Sunday. Build-it-yourself
specs, no full solutions. Airflow 3 only.

Session ritual: **concept -> API + example -> build spec -> production tip -> verify & push**.

Artifacts per session:
- Concept note: `docs/course/WW-topic.md` (repo) + local `Documents/AIRFLOW/course/WW-topic.md`.
- Your DAG: `dags/WW_topic.py`, must pass integrity tests + CI.

Tick each session when its CI-green commit lands.

---

## Phase A - Authoring surface
- [ ] **01 - W1 Sat** - TaskFlow `@dag`/`@task`, return->XCom, `multiple_outputs`
- [ ] **02 - W1 Sun** - Classic operators + `>>`, `chain`, `chain_linear`, `cross_downstream`
- [ ] **03 - W2 Sat** - TaskGroups: `@task_group`, nesting, `group_id`
- [ ] **04 - W2 Sun** - Dynamic task mapping: `.expand`, `.partial`, `.expand_kwargs`, `.map`, `.zip`
- [ ] **05 - W3 Sat** - Params + Jinja + context: `Param`, `template_fields`, `get_current_context`
- [ ] **06 - W3 Sun** - Branching + trigger rules: `@task.branch`, `@task.short_circuit`, `TriggerRule`

## Phase B - Scheduling & data-awareness
- [ ] **07 - W4 Sat** - Schedules, cron, data intervals, `logical_date`, `catchup`
- [ ] **08 - W4 Sun** - Timetables: `CronTriggerTimetable` vs `CronDataIntervalTimetable`, custom
- [ ] **09 - W5 Sat** - Backfill (native): `airflow backfill create`, `--reprocess-behavior`, `max_active_runs`
- [ ] **10 - W5 Sun** - Assets: `@asset`, `Asset`, `outlets`/`inlets`, asset-aware `schedule=[...]`
- [ ] **11 - W6 Sat** - Asset logic: `AssetAll`/`AssetAny`, `AssetAlias`, partitions
- [ ] **12 - W6 Sun** - Event-driven: `AssetWatcher`, message-queue triggers, `AssetOrTimeSchedule`

## Phase C - Data passing, connections, sensors
- [ ] **13 - W7 Sat** - XCom deep: keys, `multiple_outputs`, scope
- [ ] **14 - W7 Sun** - Custom XCom backend + `ObjectStoragePath`
- [ ] **15 - W8 Sat** - Connections + Hooks: `BaseHook.get_connection`, `conn_id`, custom hook
- [ ] **16 - W8 Sun** - Variables + Secrets backends, Jinja access
- [ ] **17 - W9 Sat** - Sensors: poke vs reschedule, `FileSensor`, `ExternalTaskSensor`, `@task.sensor`
- [ ] **18 - W9 Sun** - Deferrable operators + Triggerer: `self.defer`, `BaseTrigger`, custom trigger

## Phase D - Isolation, containers, cross-DAG
- [ ] **19 - W10 Sat** - `PythonVirtualenvOperator`, `ExternalPythonOperator`
- [ ] **20 - W10 Sun** - `DockerOperator`, `KubernetesPodOperator` (+ deferrable KPO)
- [ ] **21 - W11 Sat** - Cross-DAG: `TriggerDagRunOperator`, Assets vs `ExternalTaskSensor`
- [ ] **22 - W11 Sun** - Setup/teardown: `@setup`, `@teardown`, `.as_teardown`

## Phase E - Architecture & scaling
- [ ] **23 - W12 Sat** - Architecture: Scheduler, DAG Processor, API Server, Task Execution API, bundles
- [ ] **24 - W12 Sun** - Executors: Local/Celery/Kubernetes/Edge + multiple-executor routing
- [ ] **25 - W13 Sat** - Concurrency hierarchy: `parallelism` -> pools -> `max_active_tasks` -> `max_active_runs` -> `priority_weight`
- [ ] **26 - W13 Sun** - Parsing performance + scheduler HA
- [ ] **27 - W14 Sat** - DAG versioning + DAG bundles
- [ ] **28 - W14 Sun** - Reliability: retries, timeouts, Deadline Alerts, `depends_on_past`

## Phase F - Operations & production quality
- [ ] **29 - W15 Sat** - Alerting: `on_failure_callback`, `BaseNotifier` (Slack), listeners, cluster policies
- [ ] **30 - W15 Sun** - Observability: remote logging, StatsD/OpenTelemetry, health checks, OpenLineage
- [ ] **31 - W16 Sat** - Data quality: SQL check operators, `ShortCircuitOperator` circuit breaker
- [ ] **32 - W16 Sun** - Testing + CI/CD: `dag.test()`, integrity tests, ruff AIR3, mypy, GH Actions

## Phase G - Integration & capstone
- [ ] **33 - W17 Sat** - dbt via Cosmos: `DbtDag`/`DbtTaskGroup`
- [ ] **34 - W17 Sun** - Warehouse push-down ELT: `BigQueryInsertJobOperator`
- [ ] **35 - W18 Sat** - Multi-tenancy: teams, DAG-bundle ownership, RBAC/auth managers, `queue`
- [ ] **36 - W18 Sun** - Capstone: end-to-end pipeline

---

## Through-line project

One growing pipeline, not 36 toys:

```
Public API -> dynamic-mapped extract (partitioned by data_interval)
  -> GCS Bronze (path via XCom/ObjectStorage)
  -> Asset-triggered transform DAG
  -> BigQuery + dbt (Silver/Gold)
  -> data-quality circuit breaker + Slack alerting
  -> CI/CD gating, DAG versioning, tuned concurrency
```
