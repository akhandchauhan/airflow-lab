# Not used by the Codespace, which pip-installs Airflow directly into the
# container interpreter (see .devcontainer/setup.sh).
#
# Kept for the optional Docker path: install the Astro CLI, delete this
# comment block, and run `astro dev start` to get the full five-container
# stack (Postgres metadata DB, separate scheduler / triggerer / DAG processor /
# API server). That needs a 4-core Codespace plus the
# ghcr.io/devcontainers/features/docker-in-docker:2 devcontainer feature.
FROM astrocrpublic.azurecr.io/runtime:3.3-1
