#!/usr/bin/env bash
# Installs Airflow 3 directly into the container's Python interpreter.
#
# Why not the Astro CLI here: its standalone mode builds a uv-managed venv that
# VS Code's Python extension cannot see, leaving the editor unable to resolve
# `airflow.sdk` / `pendulum` and leaving pytest without Airflow. Installing into
# the container interpreter gives ONE environment shared by the editor, pytest,
# and the `airflow` CLI.
set -euo pipefail

AIRFLOW_VERSION="3.3.1"
PY="3.12"
CONSTRAINT="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PY}.txt"

echo "==> [1/4] Installing Apache Airflow ${AIRFLOW_VERSION} (this takes ~2 min)"
# The constraint file is mandatory. Airflow has ~600 transitive dependencies and
# an unconstrained install resolves to a broken combination.
pip install --no-cache-dir --disable-pip-version-check \
  "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT}"

echo "==> [2/4] Installing dev tooling"
pip install --no-cache-dir --disable-pip-version-check \
  "apache-airflow-providers-standard" pytest ruff

if [ -s requirements.txt ]; then
  echo "==> Installing project requirements.txt"
  pip install --no-cache-dir -r requirements.txt
fi

echo "==> [3/4] Initialising AIRFLOW_HOME at ${AIRFLOW_HOME}"
mkdir -p "${AIRFLOW_HOME}"
airflow version

echo "==> [4/4] Registering the airflow-creds helper"
if ! grep -q "alias airflow-creds=" "${HOME}/.bashrc" 2>/dev/null; then
  echo "alias airflow-creds='bash ${PWD}/scripts/creds.sh'" >> "${HOME}/.bashrc"
fi

cat <<'EOF'

=========================================================
 Setup complete.

   airflow standalone      # start Airflow (UI on port 8080)
   airflow-creds           # print the UI username/password
   python -m pytest tests/ -v

 Open the UI from the Ports tab - click the globe icon on
 port 8080. Do not type the forwarded URL by hand.
=========================================================
EOF
