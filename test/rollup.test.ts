import { describe, it, expect } from "vitest";
import {
  canUseRollup,
  planRollupSegments,
  rollupWindow,
  type RollupSegment,
} from "../src/db/query/rollup";

describe("canUseRollup", () => {
  it("allows time/service/level filters", () => {
    expect(
      canUseRollup({
        attributes: {},
        service: "auth",
        level: "error",
        since: new Date("2026-03-01T10:00:00Z"),
        until: new Date("2026-03-01T11:00:00Z"),
      }),
    ).toBe(true);
  });

  it("rejects message search and attribute filters", () => {
    expect(canUseRollup({ attributes: {}, q: "declined" })).toBe(false);
    expect(canUseRollup({ attributes: { region: "eu" } })).toBe(false);
  });
});

describe("rollupWindow", () => {
  it("uses only the rollup for minute-aligned bounds", () => {
    const since = new Date("2026-03-01T10:00:00.000Z");
    const until = new Date("2026-03-01T11:00:00.000Z");
    const win = rollupWindow(since, until);
    expect(win.rollupFrom.toISOString()).toBe("2026-03-01T10:00:00.000Z");
    expect(win.rollupTo.toISOString()).toBe("2026-03-01T11:00:00.000Z");
    expect(win.edge1From.getTime()).toBe(win.edge1To.getTime());
    expect(win.edge2From.getTime()).toBe(win.edge2To.getTime());
  });

  it("splits unaligned leftovers onto the raw edges", () => {
    const since = new Date("2026-03-01T10:00:15.000Z");
    const until = new Date("2026-03-01T10:02:40.000Z");
    const win = rollupWindow(since, until);
    expect(win.edge1From.toISOString()).toBe("2026-03-01T10:00:15.000Z");
    expect(win.edge1To.toISOString()).toBe("2026-03-01T10:01:00.000Z");
    expect(win.rollupFrom.toISOString()).toBe("2026-03-01T10:01:00.000Z");
    expect(win.rollupTo.toISOString()).toBe("2026-03-01T10:02:00.000Z");
    expect(win.edge2From.toISOString()).toBe("2026-03-01T10:02:00.000Z");
    expect(win.edge2To.toISOString()).toBe("2026-03-01T10:02:40.000Z");
  });

  it("reads the whole window from logs when it spans no complete minute", () => {
    const since = new Date("2026-03-01T10:00:10.000Z");
    const until = new Date("2026-03-01T10:00:40.000Z");
    const win = rollupWindow(since, until);
    expect(win.rollupFrom.getTime()).toBe(win.rollupTo.getTime());
    expect(win.edge1From.toISOString()).toBe("2026-03-01T10:00:10.000Z");
    expect(win.edge1To.toISOString()).toBe("2026-03-01T10:00:40.000Z");
    expect(win.edge2From.getTime()).toBe(win.edge2To.getTime());
  });
});

describe("planRollupSegments", () => {
  const iso = (s: RollupSegment) => [s.source, s.from.toISOString(), s.to.toISOString()];

  it("reads a whole aligned hour window from the hourly tier only", () => {
    const segs = planRollupSegments(
      new Date("2026-03-01T10:00:00.000Z"),
      new Date("2026-03-02T10:00:00.000Z"),
      "1h",
    );
    expect(segs.map(iso)).toEqual([
      ["hour", "2026-03-01T10:00:00.000Z", "2026-03-02T10:00:00.000Z"],
    ]);
  });

  it("never uses the hourly tier for sub-hour buckets", () => {
    const segs = planRollupSegments(
      new Date("2026-03-01T10:00:00.000Z"),
      new Date("2026-03-02T10:00:00.000Z"),
      "5m",
    );
    expect(segs.map((s) => s.source)).toEqual(["minute"]);
  });

  it("peels ragged edges down through minute to raw", () => {
    const segs = planRollupSegments(
      new Date("2026-03-01T10:00:15.000Z"),
      new Date("2026-03-02T11:30:40.000Z"),
      "1d",
    );
    expect(segs.map(iso)).toEqual([
      ["raw", "2026-03-01T10:00:15.000Z", "2026-03-01T10:01:00.000Z"],
      ["minute", "2026-03-01T10:01:00.000Z", "2026-03-01T11:00:00.000Z"],
      ["hour", "2026-03-01T11:00:00.000Z", "2026-03-02T11:00:00.000Z"],
      ["minute", "2026-03-02T11:00:00.000Z", "2026-03-02T11:30:00.000Z"],
      ["raw", "2026-03-02T11:30:00.000Z", "2026-03-02T11:30:40.000Z"],
    ]);
  });

  it("covers the requested window exactly, with no gaps or overlaps", () => {
    const since = new Date("2026-03-01T10:00:15.500Z");
    const until = new Date("2026-03-04T07:42:03.250Z");
    for (const bucket of ["1m", "5m", "1h", "1d"] as const) {
      const segs = planRollupSegments(since, until, bucket);
      expect(segs[0]!.from.getTime()).toBe(since.getTime());
      expect(segs[segs.length - 1]!.to.getTime()).toBe(until.getTime());
      for (let i = 1; i < segs.length; i++) {
        expect(segs[i]!.from.getTime()).toBe(segs[i - 1]!.to.getTime());
      }
    }
  });

  it("returns nothing for an empty window", () => {
    const t = new Date("2026-03-01T10:00:00.000Z");
    expect(planRollupSegments(t, t, "1m")).toEqual([]);
  });
});
