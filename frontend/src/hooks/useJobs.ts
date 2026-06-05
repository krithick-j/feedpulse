import { useQuery } from "@tanstack/react-query";
import { getJob, getTask, getTaskRecords, listJobs, listTasks, type TaskListQuery } from "../api/client";

export function useJobs() {
  return useQuery({
    queryKey: ["jobs"],
    queryFn: listJobs,
  });
}

export function useJobDetail(jobId: string | undefined) {
  return useQuery({
    queryKey: ["jobs", jobId],
    queryFn: () => getJob(jobId!),
    enabled: Boolean(jobId),
  });
}

export function useTaskDetail(jobId: string | undefined, taskId: number | undefined) {
  return useQuery({
    queryKey: ["jobs", jobId, "tasks", taskId],
    queryFn: () => getTask(jobId!, taskId!),
    enabled: Boolean(jobId) && Number.isFinite(taskId),
  });
}

export function useTaskRecords(jobId: string | undefined, taskId: number | undefined) {
  return useQuery({
    queryKey: ["jobs", jobId, "tasks", taskId, "records"],
    queryFn: () => getTaskRecords(jobId!, taskId!),
    enabled: Boolean(jobId) && Number.isFinite(taskId),
  });
}

export function useTaskList(jobId: string | undefined, query: TaskListQuery) {
  return useQuery({
    queryKey: ["jobs", jobId, "tasks", "list", query.status ?? "all", query.retriedOnly ? "retried" : "all-attempts", query.sort ?? "url"],
    queryFn: () => listTasks(jobId!, query),
    enabled: Boolean(jobId),
  });
}
