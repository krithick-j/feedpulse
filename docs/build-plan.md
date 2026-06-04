# Feedpulse Build Plan

## Intent

Build a locally runnable feed-processing stack with:

- immediate job creation from the dashboard
- concurrent URL processing with durable orchestration
- persisted task attempts and extracted records
- live job progress in the UI
- drill-down visibility into task failures

## Source Context

This repository is being implemented from the planning notes in the workspace
root. Those notes remain the detailed source of truth for the data model,
backend behavior, and frontend workflow.

## Repository Decisions

- Repository name: `feedpulse`
- UI direction: control-room dashboard with strong status contrast and dense
  operational detail
- Frontend state path: TanStack Query for REST reads, `EventSource` hook for
  live updates, mock adapter enabled by default until backend endpoints exist
- Backend path: FastAPI service scaffold first, then schema and orchestration

## Progress

### Completed

- initialized a fresh git repository under `feedpulse`
- created backend and frontend monorepo structure
- added a FastAPI scaffold with a health endpoint
- added scaffold REST and SSE job endpoints in FastAPI with an in-memory store
- added SQLAlchemy model definitions for `jobs`, `job_tasks`, `task_attempts`,
  and `records`
- added async database session scaffolding and environment-based DB settings
- added Alembic config and an initial migration matching the schema plan
- added a repository seam for job/task persistence reads and idempotent creation
- vendored the deduped 101-URL XML source manifest into the repo
- added a backend mode switch so routes can run in `mock` or `database` mode
- added backend and frontend Dockerfiles plus a first `docker-compose.yml`
- added explicit backend boot scripts for migrations, API startup, and future worker startup
- added an in-process DB-backed job simulator so `database` mode now mutates
  `jobs`, `job_tasks`, `task_attempts`, and `records`
- added Temporal client/workflow/activity/worker scaffolding based on current
  Temporal Python SDK patterns
- added Compose services for Temporal server, UI, and workers
- switched the task detail UI to use the dedicated task-records endpoint rather
  than relying only on embedded sample records
- tightened frontend live-update handling so job detail/task detail refetch when
  DB-backed SSE progress events arrive
- upgraded DB-mode SSE to emit task-level change events in addition to job
  snapshot/progress events
- made task-attempt and record writes explicitly idempotent against the schema
  constraints in both simulator and Temporal activity paths
- improved Temporal start semantics so duplicate workflow starts are handled
  cleanly and Temporal run ids are persisted when available
- extracted a dedicated job-projection query shape for DB-mode SSE instead of
  driving live updates off the full job-detail response
- moved the UI task table onto the dedicated tasks endpoint and added backend
  filter/sort query support for task reads
- switched DB-mode SSE from blind polling to Postgres `LISTEN/NOTIFY` triggered
  projection refreshes, while keeping the frontend event contract unchanged
- hardened the Temporal workflow path so unexpected execution failures mark
  incomplete tasks failed, finalize the job, and keep queue routing aligned with
  the shared runtime lane selection
- promoted the default local Compose stack to a Temporal-first DB-mode runtime,
  while keeping the simulator available as an explicit fallback
- implemented dashboard routes for jobs list, job detail, and task detail
- added mock job/task data plus a simulated live-update loop to exercise the UI
- captured the current implementation context in this document
- switched the frontend package workflow to `pnpm`
- verified the frontend with a successful `pnpm build`

### In Progress

- keep the frontend and backend response shapes aligned as `database` mode takes
  over from the in-memory path
- harden the new Temporal runtime from scaffold to practical default runtime
- keep this document updated as the live API replaces the mock adapter

### Next

1. Validate the Temporal-first runtime end to end with real Compose execution.
2. Reduce the in-process simulator to an explicit fallback-only path.
3. Promote push-based projection updates beyond the current single-channel
   notification seam.
4. Add broader automated verification around the Temporal runtime path.

## Verification Notes

- `pnpm install` completed successfully in `frontend/`
- `pnpm build` completed successfully in `frontend/`
- `python3 -m py_compile` completed successfully for the backend scaffold files
- `python3 -m py_compile` completed successfully for the new DB, Alembic, and
  repository files
- the vendored `data/xml_sources.csv` contains 101 unique URLs
- `docker compose config` completed successfully for `docker-compose.yml`
- `python3 -m py_compile` completed successfully for the new simulator service
- `docker compose config` completed successfully after promoting the Temporal-first local stack
- `python3 -m py_compile` completed successfully for the new Temporal modules
- TypeScript config was tightened to avoid emitting generated config artifacts

## UI Notes

- The jobs view includes a guarded `Start Job` action with a client-generated
  idempotency key.
- The detail view includes summary cards, progress, sortable tasks, and attempt
  history.
- Extracted records now come from the dedicated `/tasks/:task_id/records`
  endpoint, which aligns the UI with the planned backend surface.
- The task table now also reads from the dedicated `/jobs/:id/tasks` endpoint,
  with status/sort parameters instead of relying on the embedded task list from
  the job detail payload.
- DB-backed SSE now emits `task.updated` deltas by diffing task summaries
  between projection refreshes, which better matches the planned frontend
  invalidation flow.
- DB-backed SSE now reads from a dedicated projection query path (`job summary +
  task summaries`) instead of the heavier detail view, which is the clean seam
  for later projection-store work.
- Mock streaming mutates query cache through the same event shapes the live SSE
  path will consume, so replacing the transport should not require a UI rewrite.

## Backend Notes

- The Postgres schema matches the planning notes:
  `jobs`, `job_tasks`, `task_attempts`, `records`, plus native enums.
- The API now supports two runtime backends:
  `mock` for demo behavior and `database` for repository-backed reads/writes.
- Job creation in `database` mode uses the vendored deduped XML source manifest
  from `data/xml_sources.csv` rather than a hardcoded demo URL catalog.
- The default Compose stack now includes the Temporal server, Temporal UI, and
  small/large workers alongside `app-postgres`, `db-migrate`, `api`, and
  `frontend`, so local DB-mode startup is Temporal-first rather than
  simulator-first.
- In `database` mode, the in-process simulator still exists, but it is now an
  explicit fallback path rather than the intended primary runtime.
- A Temporal-backed path now exists as the main orchestration path:
  the API can start a workflow, workers can poll workflow/small/large task
  queues, and the default Compose stack wires those services together.
- The API now persists `temporal_run_id` when the Temporal client exposes it and
  treats duplicate workflow starts as a reusable condition rather than an
  unhandled orchestration error.
- Workflow task payloads now carry the selected queue, so workflow scheduling,
  worker execution, and the simulator all use the same lane-selection rule.
- `task_attempts` now use explicit `attempt_number` values and retry-safe
  insertion semantics; `records` now use conflict-ignore bulk inserts keyed by
  `(task_id, dedupe_key)` plus post-insert recounting for `records_extracted`.
- In `database` mode, the SSE endpoint now subscribes to Postgres
  `LISTEN/NOTIFY` events and refreshes the job projection only when matching
  job notifications arrive, with keepalive comments during idle periods.
- If the Temporal workflow hits an unexpected execution error, an explicit
  cleanup activity now marks unfinished tasks failed before the job is
  finalized, instead of leaving the runtime stuck in a non-terminal state.
- The new repository layer already encodes the intended read-model direction:
  derive job counts from `job_tasks`, keep idempotency on `jobs.idempotency_key`,
  and scope extracted-record dedupe to `(task_id, dedupe_key)`.
