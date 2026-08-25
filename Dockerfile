# Standalone mode reads ONLY this FROM line to decide which Airflow version
# to install into the uv venv. RUN / COPY instructions are ignored in
# standalone mode - put Python deps in requirements.txt instead.
FROM astrocrpublic.azurecr.io/runtime:3.3-1
