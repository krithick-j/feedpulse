import {
  type ExtractedRecord,
  type JobDetail,
  type JobEvent,
  type JobSummary,
  type StartJobResponse,
  type TaskAttempt,
  type TaskDetail,
  type TaskStatus,
  type TaskSummary,
} from "../types/jobs";

const taskCatalog = [
  "https://news.ycombinator.com/rss",
  "https://www.youtube.com/feeds/videos.xml?channel_id=UC_x5XG1OV2P6uZZ5FSM9Ttw",
  "https://feeds.arstechnica.com/arstechnica/index",
  "https://www.1password.com/blog/feed.xml",
  "https://www.nasa.gov/rss/dyn/breaking_news.rss",
  "https://www.cisa.gov/news.xml",
  "https://www.space.com/feeds/all",
  "https://www.bleepingcomputer.com/feed/",
  "https://www.theverge.com/rss/index.xml",
  "https://www.darkreading.com/rss.xml",
  "https://krebsonsecurity.com/feed/",
  "https://www.crowdstrike.com/blog/feed/",
];

const sampleRecords: ExtractedRecord[] = [
  {
    id: "rec-1",
    title: "Launch window opens for new orbital test",
    link: "https://example.com/launch-window",
    publishedAt: "2026-06-04T05:30:00Z",
    author: "Mission Desk",
    summary: "Mission control confirms final checks are complete.",
  },
  {
    id: "rec-2",
    title: "Threat intel brief highlights emerging campaign",
    link: "https://example.com/threat-intel",
    publishedAt: "2026-06-04T06:10:00Z",
    author: "Ops Team",
    summary: "Analysts note an increase in credential-harvesting activity.",
  },
];

function clone<T>(value: T): T {
  return JSON.parse(JSON.stringify(value)) as T;
}

function makeTask(id: number, url: string, status: TaskStatus, retried = false): TaskDetail {
  const completed = status === "completed";
  const failed = status === "failed";
  const attempts: TaskAttempt[] =
    failed
      ? [
          {
            attemptNumber: 1,
            status: "failed",
            startedAt: "2026-06-04T06:12:00Z",
            finishedAt: "2026-06-04T06:12:02Z",
            durationMs: 2200,
            httpStatus: 403,
            errorType: "HttpClientError",
            errorMessage: "403 response while fetching feed",
          },
        ]
      : retried
        ? [
            {
              attemptNumber: 1,
              status: "failed",
              startedAt: "2026-06-04T06:09:00Z",
              finishedAt: "2026-06-04T06:09:01Z",
              durationMs: 1200,
              httpStatus: 429,
              errorType: "FeedFetchError",
              errorMessage: "Timeout while fetching feed",
            },
            {
              attemptNumber: 2,
              status: completed ? "succeeded" : "running",
              startedAt: "2026-06-04T06:11:00Z",
              finishedAt: completed ? "2026-06-04T06:11:02Z" : null,
              durationMs: completed ? 1800 : null,
              httpStatus: null,
              errorType: null,
              errorMessage: null,
            },
          ]
      : [
          {
            attemptNumber: 1,
            status: completed ? "succeeded" : "running",
            startedAt: "2026-06-04T06:11:00Z",
            finishedAt: completed ? "2026-06-04T06:11:02Z" : null,
            durationMs: completed ? 1800 : null,
            httpStatus: null,
            errorType: null,
            errorMessage: null,
          },
        ];

  return {
    id,
    url,
    status,
    queue: url.includes("youtube") ? "xml-large-queue" : "xml-small-queue",
    attemptCount: attempts.length,
    recordsExtracted: completed ? 6 + (id % 5) : 0,
    durationMs: completed ? 1600 + id * 80 : failed ? 2200 : null,
    lastError: failed ? "403 response while fetching feed" : null,
    lastErrorType: failed ? "HttpClientError" : null,
    startedAt: "2026-06-04T06:11:00Z",
    finishedAt: completed || failed ? "2026-06-04T06:11:02Z" : null,
    attempts,
    sampleRecords: completed ? sampleRecords : [],
  };
}

const initialJobs: JobDetail[] = [
  {
    id: "job-2026-001",
    status: "running",
    totalUrls: 12,
    counts: {
      pending: 4,
      inProgress: 3,
      completed: 4,
      failed: 1,
    },
    createdAt: "2026-06-04T06:10:00Z",
    startedAt: "2026-06-04T06:10:02Z",
    finishedAt: null,
    elapsedMs: 224000,
    temporalRunId: "run-9fa132c2",
    live: true,
    throughputPerMinute: 2.1,
    reroutedTasks: 3,
    tasks: [
      makeTask(1, taskCatalog[0], "completed"),
      makeTask(2, taskCatalog[1], "completed", true),
      makeTask(3, taskCatalog[2], "completed"),
      makeTask(4, taskCatalog[3], "completed"),
      makeTask(5, taskCatalog[4], "failed"),
      makeTask(6, taskCatalog[5], "in_progress"),
      makeTask(7, taskCatalog[6], "in_progress"),
      makeTask(8, taskCatalog[7], "in_progress"),
      makeTask(9, taskCatalog[8], "pending"),
      makeTask(10, taskCatalog[9], "pending"),
      makeTask(11, taskCatalog[10], "pending"),
      makeTask(12, taskCatalog[11], "pending"),
    ],
  },
  {
    id: "job-2026-000",
    status: "completed_with_failures",
    totalUrls: 12,
    counts: {
      pending: 0,
      inProgress: 0,
      completed: 10,
      failed: 2,
    },
    createdAt: "2026-06-04T04:30:00Z",
    startedAt: "2026-06-04T04:30:03Z",
    finishedAt: "2026-06-04T04:33:41Z",
    elapsedMs: 218000,
    temporalRunId: "run-34231ab0",
    live: false,
    throughputPerMinute: 3.3,
    reroutedTasks: 2,
    tasks: taskCatalog.map((url, index) =>
      makeTask(index + 20, url, index === 3 || index === 8 ? "failed" : "completed", index === 5),
    ),
  },
];

const jobStore = new Map<string, JobDetail>();
const idempotencyMap = new Map<string, string>();

for (const job of initialJobs) {
  jobStore.set(job.id, clone(job));
}

function toSummary(job: JobDetail): JobSummary {
  return {
    id: job.id,
    status: job.status,
    totalUrls: job.totalUrls,
    counts: clone(job.counts),
    createdAt: job.createdAt,
    startedAt: job.startedAt,
    finishedAt: job.finishedAt,
    elapsedMs: job.elapsedMs,
    temporalRunId: job.temporalRunId,
  };
}

function recalculateJob(job: JobDetail) {
  const counts = {
    pending: 0,
    inProgress: 0,
    completed: 0,
    failed: 0,
  };

  for (const task of job.tasks) {
    if (task.status === "pending") counts.pending += 1;
    if (task.status === "in_progress") counts.inProgress += 1;
    if (task.status === "completed") counts.completed += 1;
    if (task.status === "failed") counts.failed += 1;
  }

  job.counts = counts;
  job.reroutedTasks = job.tasks.filter((task) => task.queue === "xml-large-queue").length;
  job.throughputPerMinute = Number(
    ((counts.completed + counts.failed) / Math.max(job.elapsedMs / 60_000, 1)).toFixed(1),
  );

  if (counts.pending === 0 && counts.inProgress === 0) {
    job.live = false;
    job.finishedAt = new Date().toISOString();
    job.status = counts.failed > 0 ? "completed_with_failures" : "completed";
  } else {
    job.live = true;
    job.status = "running";
  }
}

export async function listMockJobs(): Promise<JobSummary[]> {
  return [...jobStore.values()]
    .map((job) => toSummary(job))
    .sort((left, right) => right.createdAt.localeCompare(left.createdAt));
}

export async function getMockJob(jobId: string): Promise<JobDetail> {
  const job = jobStore.get(jobId);

  if (!job) {
    throw new Error(`Unknown job ${jobId}`);
  }

  return clone(job);
}

export async function getMockTask(jobId: string, taskId: number): Promise<TaskDetail> {
  const job = jobStore.get(jobId);
  const task = job?.tasks.find((item) => item.id === taskId);

  if (!job || !task) {
    throw new Error(`Unknown task ${taskId}`);
  }

  return clone(task);
}

export async function startMockJob(idempotencyKey: string): Promise<StartJobResponse> {
  const existing = idempotencyMap.get(idempotencyKey);

  if (existing) {
    return { jobId: existing, reused: true };
  }

  const createdAt = new Date().toISOString();
  const jobId = `job-${Date.now()}`;
  const nextJob: JobDetail = {
    id: jobId,
    status: "running",
    totalUrls: taskCatalog.length,
    counts: {
      pending: taskCatalog.length - 2,
      inProgress: 2,
      completed: 0,
      failed: 0,
    },
    createdAt,
    startedAt: createdAt,
    finishedAt: null,
    elapsedMs: 0,
    temporalRunId: `run-${Math.random().toString(16).slice(2, 10)}`,
    live: true,
    throughputPerMinute: 0,
    reroutedTasks: 0,
    tasks: taskCatalog.map((url, index) =>
      makeTask(index + 100, url, index < 2 ? "in_progress" : "pending"),
    ),
  };

  jobStore.set(jobId, nextJob);
  idempotencyMap.set(idempotencyKey, jobId);

  return { jobId, reused: false };
}

export function advanceMockJob(jobId: string): JobEvent[] {
  const job = jobStore.get(jobId);

  if (!job || !job.live) {
    return [];
  }

  job.elapsedMs += 6500;

  const candidate = job.tasks.find((task) => task.status === "in_progress")
    ?? job.tasks.find((task) => task.status === "pending");

  if (!candidate) {
    recalculateJob(job);
    return [
      {
        type: "job.completed",
        payload: {
          jobId: job.id,
          counts: clone(job.counts),
          elapsedMs: job.elapsedMs,
          status: job.status,
        },
      },
    ];
  }

  if (candidate.status === "pending") {
    candidate.status = "in_progress";
    candidate.startedAt = new Date().toISOString();
    candidate.attempts = [
      {
        attemptNumber: 1,
        status: "running",
        startedAt: candidate.startedAt,
        finishedAt: null,
        durationMs: null,
        httpStatus: null,
        errorType: null,
        errorMessage: null,
      },
    ];
  } else {
    candidate.status = candidate.url.includes("darkreading")
      ? "failed"
      : "completed";
    candidate.finishedAt = new Date().toISOString();
    candidate.durationMs = 1500 + (candidate.id % 7) * 140;
    candidate.attemptCount = 1;
    candidate.attempts = [
      {
        attemptNumber: 1,
        status: candidate.status === "completed" ? "succeeded" : "failed",
        startedAt: candidate.startedAt ?? new Date().toISOString(),
        finishedAt: candidate.finishedAt,
        durationMs: candidate.durationMs,
        httpStatus: candidate.status === "failed" ? 429 : null,
        errorType: candidate.status === "failed" ? "Http429Error" : null,
        errorMessage: candidate.status === "failed" ? "429 retry budget exhausted" : null,
      },
    ];
    candidate.recordsExtracted = candidate.status === "completed" ? 5 + (candidate.id % 4) : 0;
    candidate.lastError = candidate.status === "failed" ? "429 retry budget exhausted" : null;
    candidate.lastErrorType = candidate.status === "failed" ? "Http429Error" : null;
    candidate.sampleRecords = candidate.status === "completed" ? sampleRecords : [];
  }

  recalculateJob(job);

  const events: JobEvent[] = [
    {
      type: "task.updated",
      payload: {
        jobId: job.id,
        task: clone(candidate),
      },
    },
    {
      type: job.live ? "job.progress" : "job.completed",
      payload: {
        jobId: job.id,
        counts: clone(job.counts),
        elapsedMs: job.elapsedMs,
        status: job.status,
      },
    },
  ];

  return events;
}
