import { useEffect } from "react";
import { QueryClient, useQueryClient } from "@tanstack/react-query";
import { getMockEvents, normalizeJobEvent, USE_MOCK_DATA } from "../api/client";
import type { JobDetail, JobEvent, TaskDetail, TaskSummary } from "../types/jobs";

// Patch the cached task-list rows in place across every filter/sort variant,
// so a task.updated event never refetches GET /jobs/{id}/tasks. Rows that no
// longer match an active status filter (or a metric-based sort order) self-
// correct on the next real fetch (filter/sort change or remount).
function patchTaskInLists(queryClient: QueryClient, jobId: string, nextTask: TaskSummary) {
  queryClient.setQueriesData<TaskSummary[] | undefined>(
    { queryKey: ["jobs", jobId, "tasks", "list"] },
    (current) =>
      current?.map((task) => (task.id === nextTask.id ? { ...task, ...nextTask } : task)),
  );
}

function patchTaskInJob(job: JobDetail, nextTask: TaskSummary): JobDetail {
  return {
    ...job,
    tasks: job.tasks.map((task) => (task.id === nextTask.id ? { ...task, ...nextTask } : task)),
  };
}

export function useJobEvents(jobId: string | undefined, enabled: boolean) {
  const queryClient = useQueryClient();

  useEffect(() => {
    if (!jobId || !enabled) {
      return;
    }

    if (USE_MOCK_DATA) {
      const timer = window.setInterval(() => {
        const events = getMockEvents(jobId);

        for (const event of events) {
          applyEvent(queryClient, event);
        }
      }, 1800);

      return () => window.clearInterval(timer);
    }

    const eventSource = new EventSource(
      `${import.meta.env.VITE_API_BASE_URL ?? "http://localhost:8000/api/v1"}/jobs/${jobId}/events`,
    );

    const handleMessage = (message: MessageEvent<string>) => {
      const parsed = normalizeJobEvent(JSON.parse(message.data));
      applyEvent(queryClient, parsed);
    };

    eventSource.addEventListener("message", handleMessage as EventListener);

    return () => {
      eventSource.removeEventListener("message", handleMessage as EventListener);
      eventSource.close();
    };
  }, [enabled, jobId, queryClient]);
}

function applyEvent(queryClient: QueryClient, event: JobEvent) {
  if (event.type === "task.updated") {
    // Patch caches directly from the event payload instead of refetching.
    queryClient.setQueryData<JobDetail | undefined>(["jobs", event.payload.jobId], (current) => {
      if (!current) {
        return current;
      }

      return patchTaskInJob(current, event.payload.task);
    });

    queryClient.setQueryData<TaskDetail | undefined>(
      ["jobs", event.payload.jobId, "tasks", event.payload.task.id],
      (current) => (current ? { ...current, ...event.payload.task } : current),
    );

    // The task-list query is keyed by filter/sort and the records query holds
    // server-derived data the event payload doesn't carry, so those still need
    // a refetch. Both only refetch when actually mounted.
    patchTaskInLists(queryClient, event.payload.jobId, event.payload.task);

    queryClient.invalidateQueries({
      queryKey: ["jobs", event.payload.jobId, "tasks", event.payload.task.id, "records"],
    });

    return;
  }

  // Job-level snapshot/progress/completed: patch the detail cache from the
  // payload. No invalidation — setQueryData already updated the only query
  // that is mounted on this page.
  queryClient.setQueryData<JobDetail | undefined>(["jobs", event.payload.jobId], (current) => {
    if (!current) {
      return current;
    }

    return {
      ...current,
      counts: event.payload.counts,
      elapsedMs: event.payload.elapsedMs,
      status: event.payload.status,
      live: event.type !== "job.completed",
      finishedAt:
        event.type === "job.completed"
          ? new Date().toISOString()
          : current.finishedAt,
    };
  });
}
