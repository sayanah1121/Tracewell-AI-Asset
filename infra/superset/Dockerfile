FROM apache/superset:6.1.0

USER root
# Trino driver so Superset can query the Iceberg tables.
RUN . /app/.venv/bin/activate && uv pip install --no-cache trino
USER superset
