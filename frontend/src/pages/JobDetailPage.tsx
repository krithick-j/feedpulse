import { useEffect, useState } from "react";
import { Link, useLocation, useParams } from "react-router-dom";
import { TEMPORAL_UI_BASE_URL } from "../api/client";
import { MetricCard } from "../components/MetricCard";
import { ProgressBar } from "../components/ProgressBar";
import { StatusBadge } from "../components/StatusBadge";
import { type SortKey, TaskFilters } from "../components/TaskFilters";
import { TaskTable } from "../components/TaskTable";
import { useJobEvents } from "../hooks/useJobEvents";
import { isJobLive } from "../lib/jobStatus";
import { useJobDetail, useTaskDetail, useTaskList, useTaskRecords } from "../hooks/useJobs";
import type { TaskAttempt, TaskStatus } from "../types/jobs";

export function JobDetailPage() {
  const { jobId, taskId } = useParams();
  const location = useLocation();
  const [statusFilter, setStatusFilter] = useState<TaskStatus | "all">("all");
  const [sortKey, setSortKey] = useState<SortKey>("url");
  const [recordsOffset, setRecordsOffset] = useState(0);
  const parsedTaskId = taskId ? Number(taskId) : undefined;
  const recordsPageSize = 20;
  const startedState = new URLSearchParams(location.search).get("started");

  const jobQuery = useJobDetail(jobId);
  const taskQuery = useTaskDetail(jobId, parsedTaskId);
  const recordsQuery = useTaskRecords(jobId, parsedTaskId, {
    limit: recordsPageSize,
    offset: recordsOffset,
  });
  const taskListQuery = useTaskList(jobId, {
    status: statusFilter,
    sort: sortKey,
  });

  useJobEvents(jobId, isJobLive(jobQuery.data?.status));

  useEffect(() => {
    setRecordsOffset(0);
  }, [parsedTaskId]);

  if (jobQuery.isLoading) {
    return <main className="shell"><div className="empty-state">Loading job...</div></main>;
  }

  if (!jobQuery.data) {
    return <main className="shell"><div className="empty-state">Job not found.</div></main>;
  }

  const job = jobQuery.data;

  return (
    <main className="shell">
      <section className="panel detail-hero">
        <div className="detail-heading">
          <div>
            <p className="eyebrow">Job Detail</p>
            <h1>{job.id}</h1>
            <p className="hero-text">
              Trace job-level progress while keeping queue placement and task retries visible.
            </p>
          </div>

          <div className="detail-actions">
            <StatusBadge status={job.status} />
            {job.temporalRunId && (
              <a
                className="secondary-link"
                href={TEMPORAL_UI_BASE_URL}
                target="_blank"
                rel="noreferrer"
              >
                Open Temporal UI
              </a>
            )}
            <Link className="secondary-link" to="/">Back to jobs</Link>
          </div>
        </div>

        <ProgressBar
          completed={job.counts.completed}
          failed={job.counts.failed}
          total={job.totalUrls}
        />

        <div className="metrics-grid">
          <MetricCard
            label="Elapsed"
            value={formatDuration(job.elapsedMs)}
            detail={job.live ? "Still receiving events." : "Job reached a terminal state."}
          />
          <MetricCard
            label="Throughput"
            value={`${job.throughputPerMinute}/min`}
            detail="Completed and failed tasks normalized over elapsed time."
          />
          <MetricCard
            label="Rerouted"
            value={String(job.reroutedTasks)}
            detail="Tasks that were pushed to the large queue."
          />
          <MetricCard
            label="Temporal"
            value={job.temporalRunId ?? "Pending"}
            detail={(
              <>
                <span>Workflow: {job.id}</span>
                <br />
                <span>Use Temporal UI for orchestration inspection.</span>
              </>
            )}
          />
        </div>

        {startedState && (
          <div className="notice-banner">
            {startedState === "reused"
              ? "Existing job reused for the submitted idempotency key."
              : "Job accepted and processing has started."}
          </div>
        )}
      </section>

      <section className="detail-grid">
        <div className="detail-main">
          <div className="panel">
            <div className="panel-header">
              <div>
                <h2>Task View</h2>
                <p>Sort and filter the task lane without leaving the detail page.</p>
              </div>
              <TaskFilters
                statusFilter={statusFilter}
                sortKey={sortKey}
                onStatusFilterChange={setStatusFilter}
                onSortKeyChange={setSortKey}
              />
            </div>
          </div>

          {taskListQuery.isLoading ? (
            <div className="panel"><div className="empty-state">Loading task list...</div></div>
          ) : (
            <TaskTable jobId={job.id} tasks={taskListQuery.data ?? []} selectedTaskId={parsedTaskId} />
          )}
        </div>

        <aside className="panel detail-side">
          <div className="panel-header">
            <div>
              <h2>Task Detail</h2>
              <p>Attempts and sample extracted records for the selected task.</p>
            </div>
          </div>

          {!parsedTaskId ? (
            <div className="empty-state">
              Select a task row to inspect attempt history and extracted content.
            </div>
          ) : taskQuery.isLoading ? (
            <div className="empty-state">Loading task...</div>
          ) : taskQuery.data ? (
            <div className="task-detail">
              <div className="task-summary">
                <StatusBadge status={taskQuery.data.status} />
                <h3>{taskQuery.data.url}</h3>
                <p>
                  Queue: {taskQuery.data.queue ?? "Waiting"} | Attempts: {taskQuery.data.attemptCount}
                </p>
                {taskQuery.data.attemptCount > 1 && (
                  <p className="retry-note">
                    Task retried {taskQuery.data.attemptCount - 1} time{taskQuery.data.attemptCount === 2 ? "" : "s"} before the current terminal state.
                  </p>
                )}
              </div>

              <div className="attempt-list">
                {taskQuery.data.attempts.map((attempt) => (
                  <article key={attempt.attemptNumber} className="attempt-card">
                    <div className="attempt-header">
                      <div>
                        <p className="metric-label">Attempt {attempt.attemptNumber}</p>
                        <p className="attempt-status">{attemptStatusLabel(attempt)}</p>
                      </div>
                      <span className={`attempt-pill attempt-${attempt.status}`}>
                        {attempt.status === "succeeded" ? "Success" : attempt.status === "running" ? "Running" : "Failed"}
                      </span>
                    </div>

                    <div className="attempt-meta-grid">
                      <div className="attempt-meta">
                        <span className="attempt-meta-label">Started</span>
                        <span>{formatTimestamp(attempt.startedAt)}</span>
                      </div>
                      <div className="attempt-meta">
                        <span className="attempt-meta-label">Finished</span>
                        <span>{formatTimestamp(attempt.finishedAt)}</span>
                      </div>
                      <div className="attempt-meta">
                        <span className="attempt-meta-label">Duration</span>
                        <span>{formatAttemptDuration(attempt)}</span>
                      </div>
                      <div className="attempt-meta">
                        <span className="attempt-meta-label">HTTP</span>
                        <span>{attempt.httpStatus ?? "N/A"}</span>
                      </div>
                    </div>

                    {attempt.errorMessage && (
                      <div className="attempt-error">
                        <p className="metric-label">Failure Detail</p>
                        <p className="error-inline">
                          {attempt.errorType ?? "ExecutionError"}: {attempt.errorMessage}
                        </p>
                      </div>
                    )}
                  </article>
                ))}
              </div>

              <div className="record-list">
                {recordsQuery.isLoading ? (
                  <div className="empty-state compact">Loading extracted records...</div>
                ) : (recordsQuery.data?.items ?? []).length === 0 ? (
                  <div className="empty-state compact">No records captured for this task yet.</div>
                ) : (
                  <>
                    <div className="panel-header">
                      <div>
                        <h3>Extracted Records</h3>
                        <p>
                          Showing {recordsQuery.data!.offset + 1}-
                          {Math.min(
                            recordsQuery.data!.offset + recordsQuery.data!.items.length,
                            recordsQuery.data!.total,
                          )} of {recordsQuery.data!.total}
                        </p>
                      </div>
                      <div className="filters">
                        <button
                          className="secondary-link button-link"
                          disabled={recordsQuery.data!.offset === 0}
                          onClick={() => setRecordsOffset((current) => Math.max(0, current - recordsPageSize))}
                        >
                          Previous
                        </button>
                        <button
                          className="secondary-link button-link"
                          disabled={!recordsQuery.data!.hasMore}
                          onClick={() => setRecordsOffset((current) => current + recordsPageSize)}
                        >
                          Next
                        </button>
                      </div>
                    </div>
                    {recordsQuery.data!.items.map((record) => (
                      <article key={record.id} className="record-card">
                        <p className="metric-label">{record.author ?? "Unknown author"}</p>
                        <h4>
                          <a className="table-link" href={record.link} target="_blank" rel="noreferrer">
                            {record.title}
                          </a>
                        </h4>
                        <p className="row-subtle">{record.publishedAt ?? "No publish timestamp"}</p>
                        <p>{record.summary}</p>
                      </article>
                    ))}
                  </>
                )}
              </div>
            </div>
          ) : (
            <div className="empty-state">Task not found.</div>
          )}
        </aside>
      </section>
    </main>
  );
}

function formatDuration(value: number) {
  const minutes = Math.floor(value / 60_000);
  const seconds = Math.floor((value % 60_000) / 1000);
  return `${minutes}m ${seconds}s`;
}

function formatAttemptDuration(attempt: TaskAttempt) {
  if (attempt.durationMs == null) {
    return attempt.status === "running" ? "Still running" : "N/A";
  }
  if (attempt.durationMs < 1000) {
    return `${attempt.durationMs} ms`;
  }

  const seconds = attempt.durationMs / 1000;
  return `${seconds.toFixed(seconds >= 10 ? 1 : 2)} s`;
}

function formatTimestamp(value: string | null) {
  if (!value) {
    return "Pending";
  }

  return new Intl.DateTimeFormat(undefined, {
    month: "short",
    day: "numeric",
    hour: "numeric",
    minute: "2-digit",
    second: "2-digit",
  }).format(new Date(value));
}

function attemptStatusLabel(attempt: TaskAttempt) {
  if (attempt.status === "succeeded") {
    return "Completed cleanly";
  }
  if (attempt.status === "running") {
    return "In flight";
  }
  if (attempt.httpStatus) {
    return `Failed with HTTP ${attempt.httpStatus}`;
  }
  return "Execution failed";
}
