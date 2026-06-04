export type JobStatus =
  | "pending"
  | "running"
  | "completed"
  | "completed_with_failures"
  | "failed";

export type TaskStatus = "pending" | "in_progress" | "completed" | "failed";

export type QueueName = "xml-small-queue" | "xml-large-queue";

export interface JobCounts {
  pending: number;
  inProgress: number;
  completed: number;
  failed: number;
}

export interface JobSummary {
  id: string;
  status: JobStatus;
  totalUrls: number;
  counts: JobCounts;
  createdAt: string;
  startedAt: string | null;
  finishedAt: string | null;
  elapsedMs: number;
  temporalRunId: string | null;
}

export interface TaskSummary {
  id: number;
  url: string;
  status: TaskStatus;
  queue: QueueName | null;
  attemptCount: number;
  recordsExtracted: number;
  durationMs: number | null;
  lastError: string | null;
  lastErrorType: string | null;
  startedAt: string | null;
  finishedAt: string | null;
}

export interface TaskAttempt {
  attemptNumber: number;
  status: "running" | "succeeded" | "failed";
  startedAt: string;
  finishedAt: string | null;
  durationMs: number | null;
  httpStatus: number | null;
  errorType: string | null;
  errorMessage: string | null;
}

export interface ExtractedRecord {
  id: string;
  title: string;
  link: string;
  publishedAt: string | null;
  author: string | null;
  summary: string | null;
}

export interface TaskDetail extends TaskSummary {
  attempts: TaskAttempt[];
  sampleRecords: ExtractedRecord[];
}

export interface JobDetail extends JobSummary {
  live: boolean;
  throughputPerMinute: number;
  reroutedTasks: number;
  tasks: TaskDetail[];
}

export interface StartJobResponse {
  jobId: string;
  reused: boolean;
}

export type JobEvent =
  | {
      type: "job.snapshot" | "job.progress" | "job.completed";
      payload: {
        jobId: string;
        counts: JobCounts;
        elapsedMs: number;
        status: JobStatus;
      };
    }
  | {
      type: "task.updated";
      payload: {
        jobId: string;
        task: TaskSummary;
      };
    };
