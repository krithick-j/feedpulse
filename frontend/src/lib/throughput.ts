import type { JobCounts } from "../types/jobs";

// All job metrics derive from the same raw source: processed tasks + elapsed
// time. No reliance on the backend's throughput_per_minute field, so rate and
// ETA can never disagree.
export function processedTasks(counts: JobCounts): number {
  return counts.completed + counts.failed;
}

// Tasks completed (or failed) per second. 0 until any time has elapsed.
export function ratePerSecond(counts: JobCounts, elapsedMs: number): number {
  if (elapsedMs <= 0) {
    return 0;
  }
  return processedTasks(counts) / (elapsedMs / 1000);
}

// Rough seconds-to-completion via constant-rate extrapolation.
// null when it cannot be estimated yet (no progress / zero rate).
// 0 when nothing is left to process.
export function etaSeconds(
  counts: JobCounts,
  totalUrls: number,
  elapsedMs: number,
): number | null {
  const remaining = totalUrls - processedTasks(counts);
  if (remaining <= 0) {
    return 0;
  }
  const rate = ratePerSecond(counts, elapsedMs);
  if (rate <= 0) {
    return null;
  }
  return remaining / rate;
}

export function formatRatePerSecond(rate: number): string {
  return `${rate.toFixed(2)}/s`;
}

export function formatEta(seconds: number | null): string {
  if (seconds == null) {
    return "Estimating…";
  }
  if (seconds <= 0) {
    return "Done";
  }
  const total = Math.round(seconds);
  const minutes = Math.floor(total / 60);
  const secs = total % 60;
  return `~${minutes}m ${secs}s`;
}
