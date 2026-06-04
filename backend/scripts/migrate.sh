#!/bin/sh
set -eu

python /app/scripts/wait_for_tcp.py \
  --host "${WAIT_FOR_POSTGRES_HOST:-app-postgres}" \
  --port "${WAIT_FOR_POSTGRES_PORT:-5432}" \
  --name "app-postgres" \
  --timeout "${WAIT_TIMEOUT_SECONDS:-60}"

exec alembic -c /app/alembic.ini upgrade head

