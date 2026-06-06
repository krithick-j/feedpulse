# Demo Walkthrough

This walkthrough is the shortest path to demonstrating the operator flow end to
end on a local stack.

## 1. Start The Stack

```bash
docker compose up --build
```

Expected:

- `api` becomes healthy
- `frontend` becomes healthy
- `temporal-worker-workflow`, `temporal-worker-small`, and `temporal-worker-large` stay up

## 2. Trigger A Job

Open the dashboard at `http://localhost:5173`, click `Start Job`, and confirm the
app navigates to `/jobs/:id`.

Expected:

- the recent-jobs table shows the new job
- the job detail header shows a live `running` status
- the Temporal run id is populated once the workflow start is persisted

## 3. Watch Live Progress

Stay on the job detail page while the run is active.

Expected:

- progress bar advances without a page refresh
- task rows change from `pending` to `in_progress` to terminal states
- failed tasks show inline error summaries
- retrying tasks show an attempts chip

## 4. Inspect A Failed Task

Filter the task table to `Failed`, open a failed row, and inspect the task sidebar.

Expected:

- attempt cards show started/finished timestamps
- HTTP failure codes are visible for permanent fetch failures
- error type and error message are visible
- record list is empty for terminal failures

## 5. Inspect A Retried Task

Sort by `Attempts` and open a task with `attemptCount > 1`.

Expected:

- the first attempt shows a failed precursor
- the final attempt shows terminal success
- the task summary remains aligned with the final attempt state

## 6. Verify Paginated Records

Open a completed task with a high record count.

Expected:

- the sidebar shows `Showing X-Y of Z`
- `Previous` and `Next` page controls work
- the API endpoint exposes `limit`, `offset`, `total`, and `has_more`

## 7. Verify Structured Logs

```bash
docker compose logs api | tail -n 20
```

Expected JSON events include examples such as:

- `job.start.accepted`
- `job.temporal_workflow.started`
- `task.started`
- `task.completed`
- `task.failed`
- `task.records.page_served`
- `reconciliation.completed`
