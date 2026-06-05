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
- checked the deduped 101-URL XML source list into backend code for job creation
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
- replaced the synthetic Temporal activity payload generator with a real XML
  ingest path that fetches bytes, runs a `defusedxml` preflight, parses via
  `feedparser`, and normalizes records into the schema shape
- implemented dashboard routes for jobs list, job detail, and task detail
- added mock job/task data plus a simulated live-update loop to exercise the UI
- captured the current implementation context in this document
- switched the frontend package workflow to `pnpm`
- verified the frontend with a successful `pnpm build`
- added direct backend coverage for Temporal activity success, permanent
  failure, retryable fetch failure, and terminal retry exhaustion behavior
- added direct backend coverage for DB-mode SSE notification parsing and the
  streamed projection event contract
- added direct backend coverage for Temporal workflow orchestration, including
  per-task queue scheduling plus failure cleanup/finalization behavior
- expanded Temporal reconciliation from startup-only repair to a periodic API
  background loop with configurable interval
- reduced the simulator from implicit fallback behavior to an explicit opt-in
  runtime gate
- broadened Temporal reconciliation to cover missing workflows, status-specific
  closed workflows, and healthy no-op running cases
- added a live runtime smoke verifier that drives the running API end to end
  and validates job completion plus extracted-record retrieval
- added a live SSE smoke verifier that drives `/jobs/{id}/events` against the
  running stack and validates streaming progress/task/completion events
- added a live failure-path verifier and fixed failed-task detail mapping for
  real API inspection of error attempts
- upgraded the task detail UI so attempt drill-down shows retry count, attempt
  status, timestamps, duration, HTTP status, and failure detail
- added retry-aware task list controls so operators can sort by attempts and
  spot retried tasks directly in the table
- added a retried-only task filter path across API, mock data, and the UI so
  operators can isolate multi-attempt work without leaving the job view

### In Progress

- keep the frontend and backend response shapes aligned as `database` mode takes
  over from the in-memory path
- harden the new Temporal runtime from scaffold to practical default runtime
- broaden retry and reconciliation coverage around the real XML ingest path
- keep this document updated as the live API replaces the mock adapter

### Next

1. Promote push-based projection updates beyond the current single-channel
   notification seam.
2. Add broader automated verification around the Temporal runtime path.
3. Broaden the live runtime verifiers into more scenario-specific checks around
   retries and multi-attempt tasks.

## Verification Notes

- `pnpm install` completed successfully in `frontend/`
- `pnpm build` completed successfully in `frontend/`
- `python3 -m py_compile` completed successfully for the backend scaffold files
- `python3 -m py_compile` completed successfully for the new DB, Alembic, and
  repository files
- `backend/app/data/xml_sources.py` contains 101 unique source URLs
- `docker compose config` completed successfully for `docker-compose.yml`
- `python3 -m py_compile` completed successfully for the new simulator service
- `docker compose config` completed successfully after promoting the Temporal-first local stack
- `python3 -m py_compile` completed successfully for the new Temporal modules
- `python3 -m py_compile` completed successfully for the new XML ingest module
- the Temporal-first Compose runtime was exercised end to end with a real job
  (`bf44baf1-75be-44cd-af77-f31a78db7cdf`) that finished as
  `completed_with_failures` with `99` completed tasks, `2` failed tasks, and
  `5167` persisted records
- a later live job (`ce3b36ea-093b-4a69-b43b-cbfc319365db`) verified transient
  retry behavior: task `221` failed once with `FeedFetchError` and then
  succeeded on attempt `2`, while the job still finished with only the
  permanent `403` failure
- API startup reconciliation was validated against the previously stranded job
  `61d7d7b9-2507-403e-96bd-5f934b3268ff`: its Temporal workflow was terminated
  and the DB job/task rows were repaired from `running + 101 pending` to
  terminal failure state
- backend unit coverage now exists for XML normalization and startup
  reconciliation under `backend/tests/`
- `docker compose exec -T api python -m unittest discover -s /app/tests`
  now completes successfully with `23` passing backend tests
- `docker compose exec -T api python /app/scripts/verify_live_runtime.py --base-url http://127.0.0.1:8000/api/v1`
  completed successfully against the live stack, producing job
  `14db7a61-a4c3-4558-ae9f-64af9f6e606d` with `100` completed tasks, `1`
  failed task, and a verified completed task record fetch
- `docker compose exec -T api python /app/scripts/verify_live_sse.py --base-url http://127.0.0.1:8000/api/v1`
  completed successfully against the live stack, producing job
  `d5ecdcff-2aa3-4b4b-9552-ddc2d359d534` with `204` progress events,
  `202` task update events, and a terminal `completed_with_failures` SSE flow
- `docker compose exec -T api python /app/scripts/verify_live_failures.py --base-url http://127.0.0.1:8000/api/v1`
  completed successfully against the live stack, producing job
  `7e03577c-e38a-49f4-a88c-05b01e337b4b` and verifying failed task `633`
  returns attempt/error metadata with `HttpClientError` and HTTP `403`
- TypeScript config was tightened to avoid emitting generated config artifacts

## UI Notes

- The jobs view includes a guarded `Start Job` action with a client-generated
  idempotency key.
- The detail view includes summary cards, progress, sortable tasks, and attempt
  history.
- Task attempt cards now surface operator-facing retry detail instead of only a
  thin status line: retry count, started/finished timestamps, duration, HTTP
  status, and explicit failure blocks are visible in the sidebar.
- The task table now exposes retries at a glance instead of burying them as a
  plain number: operators can sort by attempts and see a retried marker in-row.
- The task view now also supports a retried-only filter in both mock and live
  modes, which makes multi-attempt tasks a first-class operator slice.
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
- Job creation in `database` mode uses the provided deduped XML source list
  from `backend/app/data/xml_sources.py` rather than a hardcoded demo URL catalog.
- The default Compose stack now includes the Temporal server, Temporal UI, and
  small/large workers alongside `app-postgres`, `db-migrate`, `api`, and
  `frontend`, so local DB-mode startup is Temporal-first rather than
  simulator-first.
- In `database` mode, the in-process simulator still exists, but it is now an
  explicit opt-in fallback path guarded by `ENABLE_SIMULATOR_RUNTIME=true`
  rather than a peer default runtime.
- A Temporal-backed path now exists as the main orchestration path:
  the API can start a workflow, workers can poll workflow/small/large task
  queues, and the default Compose stack wires those services together.
- The Temporal activity path now fetches and parses real XML feeds into the
  `records` schema shape using `httpx` + `defusedxml` + `feedparser`, while the
  simulator still exists as the fallback runtime path and source of synthetic
  demo records.
- Timeout-class fetch failures now record failed attempts and flow through
  Temporal retries before the task is marked terminal; this was validated
  against a live run where one task succeeded on attempt `2`.
- On API startup, the backend now reconciles `running` DB jobs against Temporal
  workflow state and force-repairs the specific "still running in Temporal but
  zero task progress after grace window" failure mode that surfaced during live
  validation.
- The API now keeps a periodic reconciliation loop alive while the service is
  running, so stale Temporal/DB divergence is not limited to a one-time
  startup repair pass. The interval is controlled through
  `JOB_RECONCILIATION_INTERVAL_SECONDS`.
- Closed Temporal workflows are now repaired with status-specific error types
  such as `WorkflowTerminatedWithoutFinalization`, instead of collapsing every
  closed-workflow case into one generic reconciliation error.
- `backend/tests/test_xml_ingest.py` now verifies RSS/Atom normalization through
  the real ingest path, and `backend/tests/test_job_reconciler.py` covers the
  stale-job termination/repair flow plus reconciliation enablement and periodic
  loop behavior.
- `backend/tests/test_job_reconciler.py` now also covers missing-workflow
  repair, status-specific closed-workflow repair, and the no-op path for
  healthy running workflows.
- `backend/tests/test_temporal_activities.py` now covers the real Temporal
  activity branch behavior for success, permanent HTTP failure, retryable
  transport failure, and terminal retry exhaustion.
- `backend/tests/test_job_events.py` now covers Postgres job-notification
  parsing plus the DB-mode SSE event stream contract for initial snapshots,
  task deltas, progress updates, and terminal completion events.
- `backend/tests/test_task_list_filters.py` now covers the retried-only task
  filter path in mock mode and the repository call contract in database mode.
- `backend/tests/test_temporal_workflows.py` now covers workflow-level
  orchestration semantics: queue-specific activity scheduling on the success
  path and cleanup/finalization on the failure path.
- `backend/tests/test_job_start_runtime.py` now covers DB runtime selection:
  Temporal remains the default path, while the simulator requires explicit
  enablement before it can be scheduled.
- `backend/scripts/verify_live_runtime.py` now provides an end-to-end smoke
  verifier for the running API/runtime path, including job start, terminal
  polling, completed-task lookup, and extracted-record retrieval.
- `backend/scripts/verify_live_sse.py` now provides a live SSE verifier for the
  running `/events` path, including initial snapshot, progress/task updates,
  and terminal completion flow checks.
- `backend/scripts/verify_live_failures.py` now provides a live failure-path
  verifier for failed-task listing, task detail, and attempt/error inspection.
- `backend/tests/test_repository_task_detail.py` now guards the failed-task
  detail mapping path that previously crashed with duplicate `attempts`
  arguments.
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
