import type { JobStatus, TaskStatus } from "../types/jobs";

const labelMap: Record<JobStatus | TaskStatus, string> = {
  pending: "Pending",
  running: "Running",
  completed: "Completed",
  completed_with_failures: "Completed With Failures",
  failed: "Failed",
  in_progress: "In Progress",
};

export function StatusBadge({ status }: { status: JobStatus | TaskStatus }) {
  return (
    <span className={`status-badge status-${status}`}>
      {labelMap[status]}
    </span>
  );
}

