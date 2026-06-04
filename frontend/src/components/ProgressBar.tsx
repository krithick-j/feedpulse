interface ProgressBarProps {
  completed: number;
  failed: number;
  total: number;
}

export function ProgressBar({ completed, failed, total }: ProgressBarProps) {
  const completedWidth = total === 0 ? 0 : (completed / total) * 100;
  const failedWidth = total === 0 ? 0 : (failed / total) * 100;

  return (
    <div className="progress-shell">
      <div className="progress-track">
        <div className="progress-completed" style={{ width: `${completedWidth}%` }} />
        <div className="progress-failed" style={{ width: `${failedWidth}%` }} />
      </div>
      <div className="progress-label">
        <span>{completed} completed</span>
        <span>{failed} failed</span>
        <span>{total} total</span>
      </div>
    </div>
  );
}

