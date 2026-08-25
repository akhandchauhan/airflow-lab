#!/usr/bin/env bash
# Installs Airflow 3 directly into the container's Python interpreter.
#
# Why not the Astro CLI here: the Astro CLI's standalone mode builds its own
# uv-managed venv that VS Code's Python extension cannot see. That leaves the
# editor unable to resolve `airflow.sdk` / `pendulum`, and leaves `pytest`
# without Airflow. Installing into the container interpreter means ONE
# environment that the editor, pytest, and the `airflow` CLI all share.
set -euo pipefail

AIRFLOW_VERSION="3.3.1"
PY="3.12"
CONSTRAINT="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PY}.txt"

echo "==> [1/3] Installing Apache Airflow ${AIRFLOW_VERSION} (this takes ~2 min)"
# The constraint file is mandatory. Airflow has ~600 transitive dependencies
# and an unconstrained install resolves to a broken combination.
pip install --no-cache-dir --disable-pip-version-check \
  "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT}"

echo "==> [2/3] Installing dev tooling"
pip install --no-cache-dir --disable-pip-version-check \
  "apache-airflow-providers-standard" pytest ruff

if [ -s requirements.txt ]; then
  echo "==> Installing project requirements.txt"
  pip install --no-cache-dir -r requirements.txt
fi

echo "==> [3/3] Initialising AIRFLOW_HOME at ${AIRFLOW_HOME}"
mkdir -p "${AIRFLOW_HOME}"
airflow version

cat <<'EOF'

=========================================================
 Setup complete. Start Airflow with:

     airflow standalone

 Then open the forwarded port 8080 (Ports tab).
 Username is `admin`; the password prints to the terminal
 and is also written to:
     $AIRFLOW_HOME/simple_auth_manager_passwords.json.generated

 Run the test suite with:

     pytest tests/ -v
=========================================================
EOF
