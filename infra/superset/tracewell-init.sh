#!/bin/bash
# Initialise Superset's metadata DB and admin user (idempotent), then start the web server.
set -e
superset db upgrade
superset fab create-admin \
  --username admin --firstname AIRS --lastname Admin \
  --email admin@tracewell.local --password "${SUPERSET_ADMIN_PASSWORD}" || true
superset init
exec /usr/bin/run-server.sh
