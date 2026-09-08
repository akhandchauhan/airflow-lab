# Authoring rules — airflow-lab course

Hard rules for writing course notes (`docs/course/*.md`) and their reference DAGs.
`CLAUDE.md` imports this file. Each rule is numbered so it can be cited. Break none.

## Cadence & structure
- **R1** — Daily **bytes**, ~20 min/day, every day. No weekend / "Sat" / "Sun" framing anywhere.
- **R2** — Split each topic into short numbered sections; **one section = one ~20-min byte**.
- **R3** — The **points/streak scoreboard lives ONLY in `docs/course/README.md`** (learn byte = 10 pts, build byte = 20 pts). Update it when the user finishes a byte.
- **R4** — **No plan / cadence / points / ritual boilerplate inside topic notes.** That meta is README-only. A note = teaching content + its build spec.

## Teaching depth
- **R5** — Deep conceptual theory **before** API: the mechanism (parse-time vs run-time, what an object actually is, why it exists). No shallow bullets.
- **R6** — Explain **every non-stdlib library** the first time it appears (what it is, why Airflow uses it).
- **R7** — Simple English + **one running analogy** per topic. Plain sentences, not dense.

## Code examples
- **R8** — **Concept snippets highlight ONE mechanic.** Drop the `@dag`/`pipeline()`/imports wrapper; mark the key line (`# ← THE MECHANIC`). BUT every name the snippet references **must be defined in that snippet** — no calls to undefined tasks.
- **R9** — **Exactly ONE complete runnable DAG per topic**: the "Complete runnable reference" (full `@dag` + `pipeline()` + a `airflow dags test …` run line). From topic 04 on it uses **BigQuery**.
- **R10** — **After each concept sub-section, add a `🎯 Challenge`** — a short problem statement (not a solution) that reuses a concept from a **previous** session; name which one.
- **R11** — **Build spec = PROBLEM STATEMENT**, never a solution walkthrough. State what/constraints/acceptance; the user designs the how. Full code lives only in the reference (a different example).

## Naming
- **R12** — Names **clearly distinct — not identical AND not scrambled twins**. Test: "would a tired reader instantly tell these apart?" Convention: `task_id` = a noun (`full_reload`); the function = a verb form (`run_full_reload`); the variable = its role (`path`, `guard`). `@task.branch` returns the **noun `task_id` string**, never the function name. `.override`/`.expand`/`.partial` are called on the **variable**.

## Airflow / BigQuery
- **R13** — **Airflow 3 only.** Public API `airflow.sdk`. Default **TaskFlow** unless the topic is classic operators. No Airflow 2 comparisons.
- **R14** — DAGs live in `dags/<topic>/` subfolders; give correct paths in notes.
- **R15** — Every DAG passes the integrity gates (`tests/dags/test_dag_integrity.py`): real `owner`, `retries >= 1`, non-empty `tags`. Keep CI green.
- **R16** — From topic 04 on, the **reference DAG and build spec use real BigQuery** via `google_cloud_default`.
- **R17** — **Cost safety:** cap every query with `maximumBytesBilled`; no `SELECT *` on big tables; prefer `COUNT`/aggregates (0 bytes); pin providers; **never** Cloud Composer.
- **R18** — **Never commit the service-account key** (git-ignored, kept in `~/.gcp/`).
- **R19** — Connection `google_cloud_default` must be **defined** (DB `airflow connections add`, or `AIRFLOW_CONN_GOOGLE_CLOUD_DEFAULT`). `GOOGLE_APPLICATION_CREDENTIALS` alone raises `AirflowNotFoundException` in Airflow 3.
- **R20** — **Orchestrate, don't compute.** `BigQueryInsertJobOperator` XCom = the job id, not rows. Pull only small scalars back (via `@task` + `BigQueryHook.get_first`).
- **R21** — **One production tip per topic.**

## Engagement — missions & story (why the course exists this way)
- **R23** — **Every topic is a "Mission" with a cold open.** Title = `Mission NN · <memorable name>` (e.g. "Give the Pipeline a Brain"). Open with a **📟 Cold open**: a 2–4 line on-call scenario with real stakes (something broke / someone's waiting on a number), then a one-line "today's mission." The concept is taught as *the fix for that scenario*, never as abstract API.
- **R24** — **One project spine: Stack Overflow Product Health.** From Mission 04 on, every reference DAG and build uses `bigquery-public-data.stackoverflow` (`posts_questions`, `posts_answers`, `users`, `tags`, `votes`) and adds a layer to the same growing pipeline. Keep the story continuous mission-to-mission; don't invent a fresh unrelated dataset each time. (`posts_questions.tags` is pipe-delimited; `creation_date`/`answer_count` are the workhorse columns.)
- **R25** — **Production tips are war stories.** Frame the one production tip as "the bug that pages you at 2am," tied back to the cold open — concrete failure + the habit that prevents it. No dry checklist bullets.

## Format
- **R22** — Follow the `Structured` output style: answer first, headers/tables over prose, numbered concrete steps, no filler.
