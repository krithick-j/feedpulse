#!/bin/sh
set -eu

python /app/scripts/wait_for_tcp.py \
  --host "${WAIT_FOR_POSTGRES_HOST:-app-postgres}" \
  --port "${WAIT_FOR_POSTGRES_PORT:-5432}" \
  --name "app-postgres" \
  --timeout "${WAIT_TIMEOUT_SECONDS:-60}"

if [ "${WAIT_FOR_TEMPORAL:-0}" = "1" ]; then
  :
elif [ "${JOB_EXECUTION_BACKEND:-simulator}" = "temporal" ]; then
  WAIT_FOR_TEMPORAL=1
fi

if [ "${WAIT_FOR_TEMPORAL:-0}" = "1" ]; then
  python /app/scripts/wait_for_tcp.py \
    --host "${TEMPORAL_HOST:-temporal}" \
    --port "${TEMPORAL_PORT:-7233}" \
    --name "temporal" \
    --timeout "${WAIT_TIMEOUT_SECONDS:-60}"
fi

exec uvicorn app.main:app \
  --host 0.0.0.0 \
  --port "${API_PORT:-8000}"
