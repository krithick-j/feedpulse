import { describe, expect, it } from "vitest";
import type { JobCounts } from "../types/jobs";
import { etaSeconds, formatEta, formatRatePerSecond, ratePerSecond } from "./throughput";

const counts = (over: Partial<JobCounts> = {}): JobCounts => ({
  pending: 0,
  inProgress: 0,
  completed: 0,
  failed: 0,
  ...over,
});

describe("ratePerSecond", () => {
  it("counts completed + failed over elapsed seconds", () => {
    // 4 processed (3 done + 1 failed) in 2s -> 2/s
    expect(ratePerSecond(counts({ completed: 3, failed: 1 }), 2000)).toBe(2);
  });

  it("returns 0 before any time elapses (no divide-by-zero)", () => {
    expect(ratePerSecond(counts({ completed: 5 }), 0)).toBe(0);
  });
});

describe("etaSeconds", () => {
  it("extrapolates remaining work at current rate", () => {
    // 2 processed in 2s -> 1/s, 8 remaining of 10 -> 8s
    expect(etaSeconds(counts({ completed: 2 }), 10, 2000)).toBe(8);
  });

  it("returns 0 when nothing remains", () => {
    expect(etaSeconds(counts({ completed: 10 }), 10, 5000)).toBe(0);
  });

  it("returns null when rate is zero (cannot estimate yet)", () => {
    expect(etaSeconds(counts(), 10, 0)).toBeNull();
    expect(etaSeconds(counts(), 10, 3000)).toBeNull();
  });
});

describe("formatters", () => {
  it("formats rate to 2 decimals per second", () => {
    expect(formatRatePerSecond(1.857)).toBe("1.86/s");
  });

  it("formats eta, null, and done", () => {
    expect(formatEta(125)).toBe("~2m 5s");
    expect(formatEta(null)).toBe("Estimating…");
    expect(formatEta(0)).toBe("Done");
  });
});
