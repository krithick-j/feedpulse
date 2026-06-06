import type { JobStatus } from "../types/jobs";

// Mirrors the backend's _job_is_terminal (app/services/jobs.py).
export const TERMINAL_JOB_STATUSES: JobStatus[] = [
  "completed",
  "completed_with_failures",
  "failed",
];

export function isTerminalJobStatus(status: JobStatus): boolean {
  return TERMINAL_JOB_STATUSES.includes(status);
}

// Job is "live" (worth subscribing to SSE) while not yet terminal.
// Includes "pending" so the worker-lag window before set_job_running
// still opens the event stream.
export function isJobLive(status: JobStatus | undefined | null): boolean {
  return status != null && !isTerminalJobStatus(status);
}
