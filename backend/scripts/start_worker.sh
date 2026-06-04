#!/bin/sh
set -eu

python /app/scripts/wait_for_tcp.py \
  --host "${WAIT_FOR_POSTGRES_HOST:-app-postgres}" \
  --port "${WAIT_FOR_POSTGRES_PORT:-5432}" \
  --name "app-postgres" \
  --timeout "${WAIT_TIMEOUT_SECONDS:-60}"

if [ "${WAIT_FOR_TEMPORAL:-1}" = "1" ]; then
  python /app/scripts/wait_for_tcp.py \
    --host "${TEMPORAL_HOST:-temporal}" \
    --port "${TEMPORAL_PORT:-7233}" \
    --name "temporal" \
    --timeout "${WAIT_TIMEOUT_SECONDS:-60}"
fi

if [ "$#" -eq 0 ]; then
  echo "Worker command is not wired yet. Pass the final worker command to start_worker.sh." >&2
  exit 64
fi

exec "$@"

