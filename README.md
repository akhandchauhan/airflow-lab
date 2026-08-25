# airflow-lab

Apache **Airflow 3.3** learning lab. Runs in a free GitHub Codespace with no
Docker, no local install, and no cloud spend.

---

## Launch

1. Click **Code ▾ → Codespaces → Create codespace on main**
2. Wait ~2 min while `.devcontainer/setup.sh` installs `uv` and the Astro CLI
3. In the Codespace terminal:

   ```bash
   astro dev start
   ```

4. Accept the port-8080 forward prompt. Login credentials print to the terminal.

First `astro dev start` takes 2-3 minutes while uv resolves Airflow. Later
starts are seconds.

---

## Why standalone mode

`astro config set dev.mode standalone` (already applied by the setup script)
runs Airflow in a uv venv backed by SQLite instead of a five-container Docker
stack.

| | Standalone | Docker mode |
|---|---|---|
| RAM | ~1.5 GB | ~5 GB |
| Fits free 2-core Codespace | yes | tight |
| Cold start | seconds | ~60 s |
| Parallel task execution | **yes** (LocalExecutor) | yes |
| Postgres metadata DB | no (SQLite) | yes |
| DockerOperator / KubernetesPodOperator | no | yes |

Airflow 3 defaults to `LocalExecutor`, so fan-out and dynamic task mapping
execute genuinely in parallel here - the old SequentialExecutor bottleneck
does not apply.

To switch to the full container stack later, bump `hostRequirements.cpus` to
`4` in `.devcontainer/devcontainer.json`, add the
`ghcr.io/devcontainers/features/docker-in-docker:2` feature, and drop the
`astro config set` line from the setup script.

---

## Daily commands

| Action | Command |
|---|---|
| Start | `astro dev start` |
| Stop | `astro dev stop` |
| Hard reset (wipes metadata DB) | `astro dev kill` |
| Airflow CLI | `astro dev run dags list` |
| Run tests | `uv run pytest tests/ -q` |
| Lint | `uv run ruff check dags/ --select AIR3` |
| Custom port | `astro dev start --port 8081` |

---

## Layout

```
dags/        DAG definitions - wiring only, no business logic
include/     Business logic. Not parsed by Airflow, so unit-testable
tests/       pytest suite, runs in CI on every push
plugins/     Custom operators, hooks, macros
Dockerfile   Pins the Airflow version (standalone reads only the FROM line)
```

---

## DAGs

| DAG | Teaches |
|---|---|
| `taskflow_etl` | Return values as XCom, functional dependency building |
| `dynamic_multi_source_ingest` | `.partial()` / `.expand()`, idempotent dated partitions |

---

## Airflow 3 syntax that replaced Airflow 2

Airflow 2 reached end of life on **22 April 2026**. These are hard errors now:

| Airflow 2 | Airflow 3 |
|---|---|
| `schedule_interval=` | `schedule=` |
| `context["execution_date"]` | `context["logical_date"]` |
| `from airflow import DAG` | `from airflow.sdk import DAG` |
| `from airflow.operators.python import ...` | `from airflow.providers.standard.operators.python import ...` |
| `catchup=True` default | `catchup=False` default |

Also changed: `logical_date` now equals `run_after`, **not**
`data_interval_start`. For incremental loads always use `data_interval_start`
and `data_interval_end`, never `logical_date` and never `datetime.now()`.

---

## Cost

Everything here stays inside free quotas.

- Codespaces: 2-core burns 2 core-hours per wall-clock hour
- Idle timeout is 30 min; `astro dev start` keeps the machine active, so
  **stop the codespace when you finish**, don't just close the tab
- Stopped codespaces still consume the storage quota - delete ones you're done with
- Public repo, so GitHub Actions minutes are free and unmetered
