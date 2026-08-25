#!/usr/bin/env bash
# Provisions the Codespace for Airflow 3 in Astro CLI *standalone* mode.
# Standalone runs Airflow in a uv-managed venv with SQLite - no Docker needed,
# so this fits comfortably on a free 2-core Codespace.
set -euo pipefail

echo "==> [1/3] Installing uv (required by standalone mode)"
curl -LsSf https://astral.sh/uv/install.sh | sh
export PATH="$HOME/.local/bin:$PATH"
uv --version

echo "==> [2/3] Installing the Astro CLI"
curl -sSL install.astronomer.io | sudo bash -s
astro version

echo "==> [3/3] Setting standalone as the default dev mode"
# Without this you must pass --standalone to every start/stop/restart/kill.
astro config set dev.mode standalone

cat <<'EOF'

=========================================================
 Setup complete.

   astro dev start     # installs Airflow 3.3 + boots it

 First boot takes ~2-3 min while uv resolves Airflow.
 The UI opens on forwarded port 8080; login creds print
 to this terminal.
=========================================================
EOF
