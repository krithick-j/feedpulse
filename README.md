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
but it is no longer the intended primary DB-mode runtime.

The Temporal execution path now includes:

- workflow/activity modules under `backend/app/temporal/`
- worker entrypoint under `python -m app.temporal.worker`
- default Compose services for Temporal server, UI, and workers
- a real XML ingest module under `backend/app/services/xml_ingest.py` that fetches,
  preflights, parses, and normalizes feed records for the Temporal activity path

That path has now been exercised against the local Compose runtime with a real
job: one recent run completed with `99` successful feeds, `2` failed feeds, and
persisted thousands of extracted records into Postgres. Transient fetch failures
now retry through Temporal attempts before becoming terminal. The API startup
path also now reconciles stale `running` jobs against Temporal state so
activation failures do not strand old jobs forever. The main remaining runtime
work is hardening and broadening those recovery rules.

## Layout

```text
.
├── data/
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
still force the simulator runtime:

```bash
JOB_EXECUTION_BACKEND=simulator docker compose up --build
```

## Frontend Workflow

```bash
cd frontend
pnpm install
pnpm dev
```

The dashboard defaults to mock transport. To point it at the FastAPI scaffold,
create `frontend/.env` from `frontend/.env.example` and set
`VITE_USE_MOCK_DATA=false`.

## Backend Workflow

```bash
cd backend
uvicorn app.main:app --reload
```

Database configuration is defined in `backend/.env.example`. Alembic is scaffolded
under `backend/alembic/`, with the initial schema in
`backend/alembic/versions/20260604_0001_initial_schema.py`. The deduped XML
source manifest used for job creation lives in `data/xml_sources.csv` and is
loaded through `backend/app/data/source_manifest.py`. The Temporal activity path
now uses `httpx`, `defusedxml`, and `feedparser` to turn fetched XML bytes into
real `records` rows, while the simulator remains the fallback path.

Backend verification:

```bash
docker compose exec -T api python -m unittest discover -s /app/tests
```

That suite now covers XML normalization, startup reconciliation, and Temporal
activity retry/failure semantics.

## Next Slices

1. Reduce the simulator to a narrow fallback/dev-only role.
2. Broaden Temporal recovery and retry coverage beyond the current startup reconciliation path.
3. Expand automated coverage around the Temporal runtime and notification-driven SSE path.
