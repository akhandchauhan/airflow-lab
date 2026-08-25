# airflow-lab

Apache **Airflow 3.3** learning lab. Runs in a free GitHub Codespace with no
Docker and no cloud spend.

---

## Launch

1. **Code ▾ → Codespaces → Create codespace on main**
2. Wait ~3 min while `.devcontainer/setup.sh` pip-installs Airflow
3. In the terminal:

   ```bash
   airflow standalone
   ```

4. Open the **Ports** tab → click the globe icon on port **8080**

Username is `admin`. The password prints to the terminal and is also written to
`$AIRFLOW_HOME/simple_auth_manager_passwords.json.generated`.

> Do **not** paste the forwarded URL into a browser by hand. A Codespaces
> forwarded hostname only resolves once something is actually listening on that
> port - typing it early gives `DNS_PROBE_FINISHED_NXDOMAIN`. Always click
> through from the Ports tab.

---

## One environment, not two

Airflow is installed into the container's Python interpreter rather than a
separate venv. That means the editor, `pytest`, and the `airflow` CLI all
resolve the same packages - no unresolved-import squiggles on
`from airflow.sdk import dag, task`.

Airflow 3 defaults to `LocalExecutor`, so fan-out and dynamic task mapping run
genuinely in parallel here. The old SequentialExecutor bottleneck does not
apply.

---

## Daily commands

| Action | Command |
|---|---|
| Start Airflow | `airflow standalone` |
| Stop | `Ctrl+C` |
| Hard reset (wipes metadata DB) | `rm -rf .airflow && airflow standalone` |
| List DAGs | `airflow dags list` |
| Parse-check one DAG | `python dags/01_taskflow_etl.py` |
| Test a single task | `airflow tasks test taskflow_etl create_customer 2026-01-01` |
| Run tests | `pytest tests/ -v` |
| Lint | `ruff check dags/ --select AIR3` |

---

## Layout

```
dags/        DAG definitions - wiring only, no business logic
include/     Business logic. Not parsed by Airflow, so unit-testable
tests/       pytest suite, runs in CI on every push
Dockerfile   Unused here; kept for the optional Docker path
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
and `data_interval_end` - never `logical_date`, never `datetime.now()`.

---

## Cost

- Codespaces 2-core burns 2 core-hours per wall-clock hour
- `airflow standalone` keeps the machine active, so the 30-min idle timeout
  will **not** fire while it runs - stop the codespace explicitly when done
- Stopped codespaces still consume the storage quota; delete ones you're finished with
- Public repo, so GitHub Actions minutes are free and unmetered
