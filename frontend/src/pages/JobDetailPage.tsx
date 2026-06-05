import { useState } from "react";
import { Link, useParams } from "react-router-dom";
import { MetricCard } from "../components/MetricCard";
import { ProgressBar } from "../components/ProgressBar";
import { StatusBadge } from "../components/StatusBadge";
import { type SortKey, TaskFilters } from "../components/TaskFilters";
import { TaskTable } from "../components/TaskTable";
import { useJobEvents } from "../hooks/useJobEvents";
import { useJobDetail, useTaskDetail, useTaskList, useTaskRecords } from "../hooks/useJobs";
import type { TaskAttempt, TaskStatus } from "../types/jobs";

export function JobDetailPage() {
  const { jobId, taskId } = useParams();
  const [statusFilter, setStatusFilter] = useState<TaskStatus | "all">("all");
  const [retriedOnly, setRetriedOnly] = useState(false);
  const [sortKey, setSortKey] = useState<SortKey>("url");
  const parsedTaskId = taskId ? Number(taskId) : undefined;

  const jobQuery = useJobDetail(jobId);
  const taskQuery = useTaskDetail(jobId, parsedTaskId);
  const recordsQuery = useTaskRecords(jobId, parsedTaskId);
  const taskListQuery = useTaskList(jobId, {
    status: statusFilter,
    retriedOnly,
    sort: sortKey,
  });

  useJobEvents(jobId, jobQuery.data?.status === "running");

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
            detail="Current run id for direct workflow inspection."
          />
        </div>
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
                retriedOnly={retriedOnly}
                sortKey={sortKey}
                onStatusFilterChange={setStatusFilter}
                onRetriedOnlyChange={setRetriedOnly}
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
                ) : (recordsQuery.data ?? []).length === 0 ? (
                  <div className="empty-state compact">No records captured for this task yet.</div>
                ) : (
                  recordsQuery.data!.map((record) => (
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
                  ))
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
