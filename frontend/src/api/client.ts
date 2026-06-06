import type {
  ExtractedRecord,
  JobDetail,
  JobEvent,
  PaginatedRecords,
  JobSummary,
  StartJobResponse,
  TaskDetail,
  TaskStatus,
  TaskSummary,
} from "../types/jobs";

const API_BASE_URL = import.meta.env.VITE_API_BASE_URL ?? "http://localhost:8000/api/v1";
export const TEMPORAL_UI_BASE_URL = import.meta.env.VITE_TEMPORAL_UI_BASE_URL ?? "http://localhost:8088";

async function fetchJson<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await fetch(`${API_BASE_URL}${path}`, {
    headers: {
      "Content-Type": "application/json",
      ...(init?.headers ?? {}),
    },
    ...init,
  });

  if (!response.ok) {
    throw new Error(`Request failed with ${response.status}`);
  }

  return response.json() as Promise<T>;
}

export function createIdempotencyKey() {
  return `ui-${Date.now()}-${Math.random().toString(16).slice(2, 8)}`;
}

export interface TaskListQuery {
  status?: TaskStatus | "all";
  sort?: "url" | "status" | "duration" | "records" | "attempts";
}

export interface TaskRecordsQuery {
  limit?: number;
  offset?: number;
}

function mapCounts(payload: any) {
  return {
    pending: payload.pending,
    inProgress: payload.in_progress,
    completed: payload.completed,
    failed: payload.failed,
  };
}

function mapTaskSummary(payload: any): TaskSummary {
  return {
    id: payload.id,
    url: payload.url,
    status: payload.status,
    queue: payload.queue,
    attemptCount: payload.attempt_count,
    recordsExtracted: payload.records_extracted,
    durationMs: payload.duration_ms,
    lastError: payload.last_error,
    lastErrorType: payload.last_error_type,
    startedAt: payload.started_at,
    finishedAt: payload.finished_at,
  };
}

function mapTaskDetail(payload: any): TaskDetail {
  return {
    ...mapTaskSummary(payload),
    attempts: payload.attempts.map((attempt: any) => ({
      attemptNumber: attempt.attempt_number,
      status: attempt.status,
      startedAt: attempt.started_at,
      finishedAt: attempt.finished_at,
      durationMs: attempt.duration_ms,
      httpStatus: attempt.http_status,
      errorType: attempt.error_type,
      errorMessage: attempt.error_message,
    })),
    sampleRecords: payload.sample_records.map(mapRecord),
  };
}

function mapJobSummary(payload: any): JobSummary {
  return {
    id: payload.id,
    status: payload.status,
    totalUrls: payload.total_urls,
    counts: mapCounts(payload.counts),
    createdAt: payload.created_at,
    startedAt: payload.started_at,
    finishedAt: payload.finished_at,
    elapsedMs: payload.elapsed_ms,
    temporalRunId: payload.temporal_run_id,
  };
}

function mapJobDetail(payload: any): JobDetail {
  return {
    ...mapJobSummary(payload),
    live: payload.live,
    throughputPerMinute: payload.throughput_per_minute,
    reroutedTasks: payload.rerouted_tasks,
    tasks: payload.tasks.map(mapTaskDetail),
  };
}

function mapStartJobResponse(payload: any): StartJobResponse {
  return {
    jobId: payload.job_id,
    reused: payload.reused,
  };
}

function mapRecord(record: any): ExtractedRecord {
  return {
    id: record.id,
    title: record.title,
    link: record.link,
    publishedAt: record.published_at,
    author: record.author,
    summary: record.summary,
  };
}

function mapPaginatedRecords(payload: any): PaginatedRecords {
  return {
    items: payload.items.map(mapRecord),
    total: payload.total,
    limit: payload.limit,
    offset: payload.offset,
    hasMore: payload.has_more,
  };
}

export function normalizeJobEvent(payload: any): JobEvent {
  if (payload.type === "task.updated") {
    return {
      type: "task.updated",
      payload: {
        jobId: payload.payload.job_id,
        task: mapTaskSummary(payload.payload.task),
      },
    };
  }

  return {
    type: payload.type,
    payload: {
      jobId: payload.payload.job_id,
      counts: mapCounts(payload.payload.counts),
      elapsedMs: payload.payload.elapsed_ms,
      status: payload.payload.status,
    },
  };
}

export async function listJobs(): Promise<JobSummary[]> {
  const payload = await fetchJson<any[]>("/jobs");
  return payload.map(mapJobSummary);
}

export async function getJob(jobId: string): Promise<JobDetail> {
  return mapJobDetail(await fetchJson<any>(`/jobs/${jobId}`));
}

export async function getTask(jobId: string, taskId: number): Promise<TaskDetail> {
  return mapTaskDetail(await fetchJson<any>(`/jobs/${jobId}/tasks/${taskId}`));
}

export async function listTasks(jobId: string, query: TaskListQuery = {}): Promise<TaskSummary[]> {
  const params = new URLSearchParams();
  if (query.status && query.status !== "all") {
    params.set("status", query.status);
  }
  if (query.sort) {
    params.set("sort", query.sort);
  }
  const suffix = params.size > 0 ? `?${params.toString()}` : "";
  const payload = await fetchJson<any[]>(`/jobs/${jobId}/tasks${suffix}`);
  return payload.map(mapTaskSummary);
}

export async function getTaskRecords(
  jobId: string,
  taskId: number,
  query: TaskRecordsQuery = {},
): Promise<PaginatedRecords> {
  const params = new URLSearchParams();
  if (query.limit != null) {
    params.set("limit", String(query.limit));
  }
  if (query.offset != null) {
    params.set("offset", String(query.offset));
  }
  const suffix = params.size > 0 ? `?${params.toString()}` : "";
  const payload = await fetchJson<any>(`/jobs/${jobId}/tasks/${taskId}/records${suffix}`);
  return mapPaginatedRecords(payload);
}

export async function startJob(idempotencyKey: string): Promise<StartJobResponse> {
  return mapStartJobResponse(await fetchJson<any>("/jobs", {
    method: "POST",
    body: JSON.stringify({ idempotencyKey }),
  }));
}
