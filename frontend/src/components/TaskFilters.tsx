import type { TaskStatus } from "../types/jobs";

export type SortKey = "url" | "status" | "duration" | "records" | "attempts";

interface TaskFiltersProps {
  statusFilter: TaskStatus | "all";
  retriedOnly: boolean;
  sortKey: SortKey;
  onStatusFilterChange: (value: TaskStatus | "all") => void;
  onRetriedOnlyChange: (value: boolean) => void;
  onSortKeyChange: (value: SortKey) => void;
}

export function TaskFilters({
  statusFilter,
  retriedOnly,
  sortKey,
  onStatusFilterChange,
  onRetriedOnlyChange,
  onSortKeyChange,
}: TaskFiltersProps) {
  return (
    <div className="filters">
      <label>
        <span>Status</span>
        <select value={statusFilter} onChange={(event) => onStatusFilterChange(event.target.value as TaskStatus | "all")}>
          <option value="all">All</option>
          <option value="pending">Pending</option>
          <option value="in_progress">In Progress</option>
          <option value="completed">Completed</option>
          <option value="failed">Failed</option>
        </select>
      </label>

      <label>
        <span>Sort</span>
        <select value={sortKey} onChange={(event) => onSortKeyChange(event.target.value as SortKey)}>
          <option value="url">URL</option>
          <option value="status">Status</option>
          <option value="duration">Duration</option>
          <option value="records">Records</option>
          <option value="attempts">Attempts</option>
        </select>
      </label>

      <label className="checkbox-filter">
        <span>Retries</span>
        <span className="checkbox-row">
          <input
            type="checkbox"
            checked={retriedOnly}
            onChange={(event) => onRetriedOnlyChange(event.target.checked)}
          />
          <span>Retried only</span>
        </span>
      </label>
    </div>
  );
}
