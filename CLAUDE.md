# airflow-lab — authoring rules for the Airflow 3 daily-bytes course

This repo is a learning course. Notes live in `docs/course/`; the DAGs the user
builds live in `dags/`. These rules govern how course **notes** (`.md`) and their
**reference DAGs** are written. They override default behavior.

## Cadence — daily bytes (not weekends)

- **~20 minutes a day, every day.** No Saturday/Sunday framing anywhere — the old
  "weekend / 1 hour Sat + Sun" plan is scrapped. Never write "W1 Sat", "weekend",
  etc. in a title or note.
- **Break every topic into small "bytes"** — each one finishable in ~20 min: read
  one short section, run a tiny example, or build one small thing. A topic note has
  numbered sections; a byte = one section (or the build). Don't hand the user a
  90-minute wall in one sitting.
- **Point system lives ONLY in `docs/course/README.md`** — the scoreboard (total
  points, current/longest streak, last active) and the byte checklist. A *learn*
  byte = 10 pts, a *build-a-DAG* byte = 20 pts. When the user reports finishing a
  byte, update the README scoreboard and tick the box.
- **Do NOT repeat plan/cadence/ritual/points boilerplate inside topic notes.** That
  meta lives in README only. A topic note contains just the teaching content and its
  own build spec — no "here's how the course works", no streak talk, no ritual
  description. Keep notes lean.

## Audience

Strong data engineer (SQL, pandas, Python, GCP/BigQuery), **new to Airflow** and
its ecosystem libraries. Learns by understanding the machinery, not by memorizing
API surface. Skip beginner buildup; go deep on *why*.

## The five hard rules (never break)

1. **Deep conceptual theory, not shallow bullets.** Explain the underlying
   mechanism — parse-time vs run-time, what an object like `XComArg` actually is,
   why a design exists — before the API surface. The Concept section must be
   substantial.

2. **Explain every non-stdlib library the first time it appears.** What it is and
   why Airflow uses it (e.g. `pendulum`, `fsspec`, `celery`). Do not assume the
   user knows a tool. (They did not know `pendulum`.)

3. **One complete, runnable reference DAG BEFORE the build spec** — in the exact
   style the spec expects. Fragments/snippets are not enough. The build spec may
   only require *varying* something already shown end-to-end, never inventing a
   structure the user has never seen whole.

4. **The build spec is a PROBLEM STATEMENT, not a solution walkthrough.** State
   WHAT to build — requirements, structure in prose, constraints, acceptance
   criteria — and let the user design the HOW. Never give numbered
   "define task X returning Y, then wire `stage(run_checks(...))`, then loop with
   `.override(...)`" steps. Full code lives only in the reference (a *different*
   example), never in the spec.

5. **Names must be clearly distinct — not identical AND not scrambled twins.**
   In every example keep the Python name distinct from the `dag_id` / `group_id` /
   `task_id` it carries. Two failure modes, both banned:
   - **Identical:** `def ingest()` with `group_id="ingest"` — impossible to tell what
     `ingest.override(...)` attaches to.
   - **Scrambled / near-identical:** `def full_refresh()` with `task_id="refresh_full"`,
     or names differing by one word-swap or letter. This is *worse* than identical —
     it reads like a typo and the eye can't separate them.
   **The test is not "are the strings different?" — it is "would a tired reader
   instantly tell these apart?"** If two names are anagrams, reorderings, or differ
   only by a suffix like `_2`, that FAILS.
   **Convention to follow:** `task_id` = a plain noun (`full_refresh`); the function
   that carries it = a verb form (`run_full_refresh`); the variable holding the task
   = its role (`path`, `guard`, `large_path`). A `@task.branch` returns the **noun**
   `task_id` string, never the function name. `.override`/`.expand`/`.partial` are
   called on the Python **variable**; the id is the **string** passed in.

## Topic note structure

- A **topic note** covers one topic, split into short numbered sections so each maps
  to a ~20-min byte. Shape: concept → API + tiny example → **complete runnable
  reference** → **build spec (problem statement)** → **production tip**. The
  README's byte list points at these sections.
- **Practicals (🔷 P1, P2, …):** every 3 topics, one practical applying them on a
  real **BigQuery public dataset** (`bigquery-public-data.*`): real dataset → setup
  → complete reference → build spec → run against BigQuery → **production tip** →
  verify (rows + bytes billed).
- **Every topic ends with ONE production tip.** (One once shipped without; a miss.)
- **From Session 04 onward, exercises use BigQuery, not generic fake-data DAGs.**
  The user has GCP creds and wants production-shaped, hands-on work in every
  session. The complete reference DAG AND the build spec must run against a real
  BigQuery table (`bigquery-public-data.*` or the user's own dataset) via
  `google_cloud_default`, with every query cost-capped. Keep the *theory* simple
  (plain English + one running analogy); make the *practice* real BigQuery.
- **Every code example is a COMPLETE, runnable DAG — never a bare fragment.** Each
  code block must include imports, the `@dag` definition, the tasks, the wiring, and
  the trailing `pipeline()` call, with a `# dags/task-N/<file>.py` header comment and
  a `airflow dags test <dag_id> <date>` run line. The user must be able to copy any
  block, save it, and run it. No snippets that reference undefined names.
- **Per-section concept demos may be minimal & self-contained** (no provider) to
  isolate ONE mechanic and run in seconds — this is allowed even from Session 04 on.
  Only the **complete reference** (the combined "§ Complete runnable reference" DAG)
  and the **build spec** must be BigQuery. So a topic can have tiny no-BigQuery
  runnable demos per concept, then one full BigQuery reference that combines them.

## Airflow conventions in this repo

- **Airflow 3 only.** No Airflow 2 comparisons. Public authoring API is
  `airflow.sdk` (`@dag`, `@task`, `@task_group`). Default to **TaskFlow** style
  unless a session is specifically about classic operators.
- **DAGs live in `dags/<session>/`** subfolders (e.g. `dags/task-3/`, `dags/p1/`).
  Airflow parses `dags/` recursively. Always give correct paths in notes.
- **Every DAG must pass the integrity gates** in `tests/dags/test_dag_integrity.py`:
  real `owner`, `retries >= 1`, non-empty `tags`. CI (`.github/workflows/ci.yml`)
  runs ruff (`E,F,AIR3`) + pytest on push; keep it green.
- Use `pendulum.datetime(..., tz=...)` for `start_date`; `catchup=False`,
  `schedule=None` for manual course DAGs.

## BigQuery / GCP rules (practicals)

- **Never commit the service-account key.** It stays outside the repo (`~/.gcp/`),
  git-ignored. A leaked key = someone billing your project.
- **Connection:** `google_cloud_default` must be **defined** (DB via
  `airflow connections add`, or `AIRFLOW_CONN_GOOGLE_CLOUD_DEFAULT`). Setting
  `GOOGLE_APPLICATION_CREDENTIALS` alone raises `AirflowNotFoundException` in
  Airflow 3 — the operator looks up the connection first.
- **Cost safety, always:** cap every query with `maximumBytesBilled`; no
  `SELECT *` on big tables; prefer `COUNT(*)`/aggregates (0 bytes); pin providers.
  **Never** create a Cloud Composer environment — the Codespace is the runtime.
- **Orchestrate, don't compute.** `BigQueryInsertJobOperator` is for side-effecting
  SQL; its XCom is the **job id**, not rows. Keep data in BigQuery; pull only small
  scalars back (via a `@task` + `BigQueryHook.get_first`) when orchestration must
  decide on a value. XCom is a control-signal store, not a data pipe.

## Note format

Follow the `Structured` output style: answer first, headers/tables over prose,
numbered concrete steps for procedures, no filler.
