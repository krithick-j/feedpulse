import { Link } from "react-router-dom";
import type { TaskStatus, TaskSummary } from "../types/jobs";
import { StatusBadge } from "./StatusBadge";

interface TaskTableProps {
  jobId: string;
  tasks: TaskSummary[];
  selectedTaskId?: number;
}

export function TaskTable({ jobId, tasks, selectedTaskId }: TaskTableProps) {
  return (
    <div className="panel table-panel">
      <div className="panel-header">
        <div>
          <h2>Task Flow</h2>
          <p>Inspect queue placement, retries, and extracted record volume.</p>
        </div>
      </div>

      <div className="table-wrap">
        <table>
          <thead>
            <tr>
              <th>URL</th>
              <th>Status</th>
              <th>Queue</th>
              <th>Attempts</th>
              <th>Records</th>
              <th>Duration</th>
            </tr>
          </thead>
          <tbody>
            {tasks.map((task) => (
              <tr
                key={task.id}
                className={selectedTaskId === task.id ? "is-selected" : undefined}
              >
                <td>
                  <Link to={`/jobs/${jobId}/tasks/${task.id}`} className="table-link">
                    {task.url}
                  </Link>
                  {task.lastError && (
                    <p className="error-inline">
                      {formatError(task.status, task.lastErrorType, task.lastError)}
                    </p>
                  )}
                </td>
                <td><StatusBadge status={task.status} /></td>
                <td>{task.queue ?? "Waiting"}</td>
                <td>{task.attemptCount}</td>
                <td>{task.recordsExtracted}</td>
                <td>{task.durationMs ? `${task.durationMs} ms` : "..."}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

function formatError(status: TaskStatus, errorType: string | null, error: string) {
  if (status !== "failed") {
    return "";
  }

  return errorType ? `${errorType}: ${error}` : error;
}

