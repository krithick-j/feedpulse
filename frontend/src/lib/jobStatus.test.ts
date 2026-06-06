import { describe, expect, it } from "vitest";
import type { JobStatus } from "../types/jobs";
import { isJobLive, isTerminalJobStatus } from "./jobStatus";

describe("isJobLive", () => {
  // The bug: SSE only opened when status === "running", so a freshly-started
  // job still in "pending" (worker hadn't run set_job_running yet) never
  // opened the event stream and the UI froze. "pending" must be live.
  it("treats pending as live", () => {
    expect(isJobLive("pending")).toBe(true);
  });

  it("treats running as live", () => {
    expect(isJobLive("running")).toBe(true);
  });

  it.each<JobStatus>(["completed", "completed_with_failures", "failed"])(
    "treats terminal status %s as not live",
    (status) => {
      expect(isJobLive(status)).toBe(false);
    },
  );

  it("treats undefined/null (no job loaded) as not live", () => {
    expect(isJobLive(undefined)).toBe(false);
    expect(isJobLive(null)).toBe(false);
  });
});

describe("isTerminalJobStatus", () => {
  it("matches the backend terminal set", () => {
    expect(isTerminalJobStatus("completed")).toBe(true);
    expect(isTerminalJobStatus("completed_with_failures")).toBe(true);
    expect(isTerminalJobStatus("failed")).toBe(true);
    expect(isTerminalJobStatus("pending")).toBe(false);
    expect(isTerminalJobStatus("running")).toBe(false);
  });
});
