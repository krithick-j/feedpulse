import {
  advanceMockJob,
  getMockJob,
  getMockTask,
  listMockJobs,
  startMockJob,
} from "./mockData";
import type {
  ExtractedRecord,
  JobDetail,
  JobEvent,
  JobSummary,
  StartJobResponse,
  TaskDetail,
  TaskStatus,
  TaskSummary,
} from "../types/jobs";

const API_BASE_URL = import.meta.env.VITE_API_BASE_URL ?? "http://localhost:8000/api/v1";
export const TEMPORAL_UI_BASE_URL = import.meta.env.VITE_TEMPORAL_UI_BASE_URL ?? "http://localhost:8088";
export const USE_MOCK_DATA = import.meta.env.VITE_USE_MOCK_DATA !== "false";

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
    sampleRecords: payload.sample_records.map((record: any) => ({
      id: record.id,
      title: record.title,
      link: record.link,
      publishedAt: record.published_at,
      author: record.author,
      summary: record.summary,
    })),
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
  if (USE_MOCK_DATA) {
    return listMockJobs();
  }

  const payload = await fetchJson<any[]>("/jobs");
  return payload.map(mapJobSummary);
}

export async function getJob(jobId: string): Promise<JobDetail> {
  if (USE_MOCK_DATA) {
    return getMockJob(jobId);
  }

  return mapJobDetail(await fetchJson<any>(`/jobs/${jobId}`));
}

export async function getTask(jobId: string, taskId: number): Promise<TaskDetail> {
  if (USE_MOCK_DATA) {
    return getMockTask(jobId, taskId);
  }

  return mapTaskDetail(await fetchJson<any>(`/jobs/${jobId}/tasks/${taskId}`));
}

export async function listTasks(jobId: string, query: TaskListQuery = {}): Promise<TaskSummary[]> {
  if (USE_MOCK_DATA) {
    const job = await getMockJob(jobId);
    const status = query.status && query.status !== "all" ? query.status : undefined;
    const filtered = status ? job.tasks.filter((task) => task.status === status) : [...job.tasks];
    return sortTasks(filtered, query.sort ?? "url");
  }

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

export async function getTaskRecords(jobId: string, taskId: number): Promise<ExtractedRecord[]> {
  if (USE_MOCK_DATA) {
    const task = await getMockTask(jobId, taskId);
    return task.sampleRecords;
  }

  const payload = await fetchJson<any[]>(`/jobs/${jobId}/tasks/${taskId}/records`);
  return payload.map((record) => ({
    id: record.id,
    title: record.title,
    link: record.link,
    publishedAt: record.published_at,
    author: record.author,
    summary: record.summary,
  }));
}

export async function startJob(idempotencyKey: string): Promise<StartJobResponse> {
  if (USE_MOCK_DATA) {
    return startMockJob(idempotencyKey);
  }

  return mapStartJobResponse(await fetchJson<any>("/jobs", {
    method: "POST",
    body: JSON.stringify({ idempotencyKey }),
  }));
}

export function getMockEvents(jobId: string): JobEvent[] {
  if (!USE_MOCK_DATA) {
    return [];
  }

  return advanceMockJob(jobId);
}

function sortTasks(tasks: TaskSummary[], sortBy: NonNullable<TaskListQuery["sort"]>) {
  return [...tasks].sort((left, right) => {
    if (sortBy === "status") {
      return left.status.localeCompare(right.status) || left.url.localeCompare(right.url);
    }
    if (sortBy === "duration") {
      return (right.durationMs ?? -1) - (left.durationMs ?? -1) || left.url.localeCompare(right.url);
    }
    if (sortBy === "records") {
      return right.recordsExtracted - left.recordsExtracted || left.url.localeCompare(right.url);
    }
    if (sortBy === "attempts") {
      return right.attemptCount - left.attemptCount || left.url.localeCompare(right.url);
    }
    return left.url.localeCompare(right.url) || left.id - right.id;
  });
}
