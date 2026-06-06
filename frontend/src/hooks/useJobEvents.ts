import { useEffect } from "react";
import { QueryClient, useQueryClient } from "@tanstack/react-query";
import { getMockEvents, normalizeJobEvent, USE_MOCK_DATA } from "../api/client";
import type { JobDetail, JobEvent, TaskDetail, TaskSummary } from "../types/jobs";

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
    queryClient.invalidateQueries({
      queryKey: ["jobs", event.payload.jobId, "tasks", "list"],
    });

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
