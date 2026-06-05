# Feedpulse

Feedpulse is the local operator dashboard and processing stack for concurrent XML
feed ingestion. This repository now contains the first implementation slice:

- a clean monorepo scaffold for backend and frontend work
- a React dashboard shell for jobs, job detail, and task detail views
- a tracked build plan that records implementation context and deviations
- a FastAPI API scaffold to anchor the backend service layout

## Current State

The frontend is implemented against a mock adapter by default so the dashboard can
be exercised before the API, database, and workflow layers are wired end to end.
The backend also still exposes scaffold job, task, and SSE endpoints backed by an
in-memory store so the API contract remains easy to demo. The database layer itself
is now scaffolded with SQLAlchemy models, async session setup, and an Alembic
initial migration that mirrors the planning notes. Route handlers now support two
backend modes:

- `mock`: in-memory demo behavior, enabled by default
- `database`: repository-backed reads/writes against Postgres

In `database` mode, the local Compose runtime now boots a Temporal-first execution
path by default. The in-process simulator still exists as an explicit fallback,
but it is now opt-in and no longer behaves like a peer default DB-mode runtime.

The Temporal execution path now includes:

- workflow/activity modules under `backend/app/temporal/`
- worker entrypoint under `python -m app.temporal.worker`
- default Compose services for Temporal server, UI, and workers
- a real XML ingest module under `backend/app/services/xml_ingest.py` that fetches,
  preflights, parses, and normalizes feed records for the Temporal activity path

That path has now been exercised against the local Compose runtime with a real
job: one recent run completed with `99` successful feeds, `2` failed feeds, and
persisted thousands of extracted records into Postgres. Transient fetch failures
now retry through Temporal attempts before becoming terminal. The API now
reconciles stale `running` jobs against Temporal state on startup and through a
periodic background loop, so activation failures are not limited to one-time
repair on boot. Closed workflows are also reconciled with status-specific repair
typing instead of one generic closed-workflow bucket. The main remaining runtime
work is hardening and broadening those recovery rules further.

## Layout

```text
.
├── backend/
├── docs/
└── frontend/
```

## Compose Workflow

```bash
docker compose up --build
```

The default Compose stack now brings up the full local runtime:

- `app-postgres`
- `db-migrate`
- `api`
- `frontend`
- `temporal-postgres`
- `temporal`
- `temporal-ui`
- `temporal-worker-small`
- `temporal-worker-large`

If you need the older lighter-weight path for backend-only iteration, you can
still force the simulator runtime explicitly:

```bash
JOB_EXECUTION_BACKEND=simulator ENABLE_SIMULATOR_RUNTIME=true docker compose up --build
```

## Frontend Workflow

```bash
cd frontend
pnpm install
pnpm dev
```

The dashboard defaults to mock transport. To point it at the FastAPI scaffold,
create `frontend/.env` from `frontend/.env.example` and set
`VITE_USE_MOCK_DATA=false`. `VITE_TEMPORAL_UI_BASE_URL` controls the shortcut
links that open Temporal UI from the dashboard and job detail views.

## Backend Workflow

```bash
cd backend
uvicorn app.main:app --reload
```

Database configuration is defined in `backend/.env.example`. Alembic is scaffolded
under `backend/alembic/`, with the initial schema in
`backend/alembic/versions/20260604_0001_initial_schema.py`. The provided XML
source list used for job creation now lives in
`backend/app/data/xml_sources.py`. The Temporal activity path uses `httpx`,
`defusedxml`, and `feedparser` to turn fetched XML bytes into real `records`
rows, while the simulator remains the fallback path. Temporal reconciliation
cadence is controlled with
`JOB_RECONCILIATION_INTERVAL_SECONDS`. The simulator runtime is disabled unless
`ENABLE_SIMULATOR_RUNTIME=true`.

Backend verification:

```bash
docker compose exec -T api python -m unittest discover -s /app/tests
```

Live runtime smoke verification:

```bash
docker compose exec -T api python /app/scripts/verify_live_runtime.py --base-url http://127.0.0.1:8000/api/v1
```

Live SSE smoke verification:

```bash
docker compose exec -T api python /app/scripts/verify_live_sse.py --base-url http://127.0.0.1:8000/api/v1
```

Live failure-path verification:

```bash
docker compose exec -T api python /app/scripts/verify_live_failures.py --base-url http://127.0.0.1:8000/api/v1
```

Live idempotency verification:

```bash
docker compose exec -T api python /app/scripts/verify_live_idempotency.py --base-url http://127.0.0.1:8000/api/v1
```

Live retry-path verification:

```bash
docker compose exec -T api python /app/scripts/verify_live_retries.py --base-url http://127.0.0.1:8000/api/v1
```

Live reconciliation verification:

```bash
docker compose exec -T api python /app/scripts/verify_live_reconciliation.py --base-url http://127.0.0.1:8000/api/v1
```

That suite now covers XML normalization, startup reconciliation, Temporal
activity retry/failure semantics, and the DB-backed SSE notification/event
contract, plus workflow-level orchestration, cleanup behavior, and periodic
reconciliation loop coverage, plus DB runtime selection/simulator gating, plus
broader reconciliation branch coverage. The live smoke verifier runs against the
actual Temporal-first stack and asserts job start, terminal completion, task
results, and record retrieval through the API. The SSE verifier runs against the
same live stack and asserts `job.snapshot`, streaming progress/task updates, and
terminal `job.completed` behavior through `/events`. The failure-path verifier
asserts that failed-task listing, task detail, and attempt/error metadata are
available through the live API. The idempotency verifier asserts that repeating
`POST /jobs` with the same frontend-style `idempotencyKey` reuses the original
job instead of creating a second run. The retry-path verifier keeps launching
real jobs only if recent persisted history does not already expose a
multi-attempt task, then validates the attempt-sorted task listing plus the
detailed retry history for that task. The reconciliation verifier forces a real
workflow divergence by terminating a running workflow, then proves the live
reconciler repairs the API read model to a terminal state with the expected
workflow-derived error type.

## Next Slices

1. Broaden Temporal recovery and retry coverage beyond the currently verified reconciliation branches.
2. Broaden the live smoke verifiers into more scenario-specific runtime checks.
3. Add more live runtime coverage beyond the current retry and multi-attempt task scenarios.
