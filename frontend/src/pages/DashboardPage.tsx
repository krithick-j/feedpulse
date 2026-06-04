import { useMutation, useQueryClient } from "@tanstack/react-query";
import { Link, useNavigate } from "react-router-dom";
import { createIdempotencyKey, startJob, USE_MOCK_DATA } from "../api/client";
import { MetricCard } from "../components/MetricCard";
import { StatusBadge } from "../components/StatusBadge";
import { useJobs } from "../hooks/useJobs";

export function DashboardPage() {
  const navigate = useNavigate();
  const queryClient = useQueryClient();
  const jobsQuery = useJobs();

  const startJobMutation = useMutation({
    mutationFn: startJob,
    onSuccess: async (response) => {
      await queryClient.invalidateQueries({ queryKey: ["jobs"] });
      navigate(`/jobs/${response.jobId}`);
    },
  });

  const jobs = jobsQuery.data ?? [];
  const runningJobs = jobs.filter((job) => job.status === "running").length;
  const failedTasks = jobs.reduce((sum, job) => sum + job.counts.failed, 0);
  const rerouteSignal = jobs.reduce((sum, job) => sum + Math.max(job.counts.inProgress - 1, 0), 0);

  return (
    <main className="shell">
      <section className="hero panel">
        <div className="hero-copy">
          <p className="eyebrow">Concurrent Feed Operations</p>
          <h1>Watch the ingestion lanes without losing failure detail.</h1>
          <p className="hero-text">
            Feedpulse keeps the operator view focused on job state, queue pressure,
            and task-level evidence. The current build ships with a mock transport
            so the UI can be exercised before the live backend is wired.
          </p>
          <div className="hero-actions">
            <button
              className="primary-button"
              onClick={() => startJobMutation.mutate(createIdempotencyKey())}
              disabled={startJobMutation.isPending}
            >
              {startJobMutation.isPending ? "Starting..." : "Start Job"}
            </button>
            <span className="mode-chip">{USE_MOCK_DATA ? "Mock transport" : "API transport"}</span>
          </div>
        </div>

        <div className="hero-metrics">
          <MetricCard
            label="Live Jobs"
            value={String(runningJobs)}
            detail="Currently active workflow runs."
          />
          <MetricCard
            label="Failed Tasks"
            value={String(failedTasks)}
            detail="Permanent and exhausted failures across recent runs."
          />
          <MetricCard
            label="Queue Pressure"
            value={String(rerouteSignal)}
            detail="Rough signal from tasks still moving through the lanes."
          />
        </div>
      </section>

      <section className="panel">
        <div className="panel-header">
          <div>
            <h2>Recent Jobs</h2>
            <p>Use the latest runs to inspect retries, reroutes, and extraction volume.</p>
          </div>
        </div>

        {jobsQuery.isLoading ? (
          <div className="empty-state">Loading recent jobs...</div>
        ) : (
          <div className="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>Job</th>
                  <th>Status</th>
                  <th>Progress</th>
                  <th>Elapsed</th>
                  <th>Temporal</th>
                </tr>
              </thead>
              <tbody>
                {jobs.map((job) => (
                  <tr key={job.id}>
                    <td>
                      <Link className="table-link" to={`/jobs/${job.id}`}>
                        {job.id}
                      </Link>
                      <p className="row-subtle">
                        {new Date(job.createdAt).toLocaleString()}
                      </p>
                    </td>
                    <td><StatusBadge status={job.status} /></td>
                    <td>
                      {job.counts.completed + job.counts.failed}/{job.totalUrls}
                      <p className="row-subtle">
                        {job.counts.inProgress} active / {job.counts.pending} pending
                      </p>
                    </td>
                    <td>{formatDuration(job.elapsedMs)}</td>
                    <td>{job.temporalRunId ?? "Not started"}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </section>
    </main>
  );
}

function formatDuration(value: number) {
  const minutes = Math.floor(value / 60_000);
  const seconds = Math.floor((value % 60_000) / 1000);
  return `${minutes}m ${seconds}s`;
}

